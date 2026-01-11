import asyncio
from collections import defaultdict
from functools import wraps
import math
import warnings

from sanic.exceptions import SanicException, ServerError
from sanic.response import json, text, HTTPResponse
# from sanic.request import json_loads
from json import loads as json_loads
from sanic.views import HTTPMethodView

from sqlalchemy import Column
from sqlalchemy.exc import (DataError, IntegrityError, ProgrammingError, OperationalError)
from sqlalchemy.ext.associationproxy import AssociationProxy
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.exc import (MultipleResultsFound, NoResultFound)
from sqlalchemy.orm.query import Query

from .helpers.sqlalchemy import count
from .helpers.sqlalchemy import evaluate_functions
from .helpers.sqlalchemy import get_by
from .helpers.sqlalchemy import get_columns
from .helpers.sqlalchemy import get_or_create
from .helpers.sqlalchemy import get_related_model
from .helpers.sqlalchemy import get_relations
from .helpers.sqlalchemy import has_field
from .helpers.sqlalchemy import is_like_list
from .helpers.sqlalchemy import partition
from .helpers.sqlalchemy import primary_key_name
from .helpers.sqlalchemy import query_by_primary_key
from .helpers.sqlalchemy import session_query
from .helpers.sqlalchemy import strings_to_dates
from .helpers.sqlalchemy import to_dict
from .helpers.sqlalchemy import upper_keys
from .helpers.sqlalchemy import get_related_association_proxy_model

from .core import ModelView
from .exception import (ProcessingException, ValidationError, response_exception)
######
from inspect import getfullargspec

from sqlalchemy import (and_,or_)
from .helpers.sqlalchemy import session_query
from .helpers import to_namespace

from .constant import OPERATORS

class SqlaFilter(object):
    def __init__(self, junction="Filter", field=None, operator=None, argument=None, 
        otherfield=None, subfilters=[]):
        self.junction = junction
        self.field = field
        self.operator = operator
        self.argument = argument
        self.otherfield = otherfield
        self.subfilters = subfilters

    def __iter__(self):
        return iter(self.subfilters)
    
    def __repr__(self):
        if self.junction == "Filter":        
            return '<Filter {0} {1} {2}>'.format(self.field, 
                self.operator, self.argument or self.otherfield)
        if self.junction == "DisjunctionFilter":        
            return '<DisjunctionFilter or_{0}>'.format(tuple(repr(f) for f in self))
        if self.junction == "ConjunctionFilter":        
            return '<ConjunctionFilter and_{0}>'.format(tuple(repr(f) for f in self))

    @staticmethod
    def from_dictionary(dictionary):     
        if not dictionary:
            return None
        from_dict = SqlaFilter.from_dictionary
        if '$or' in dictionary:
            return SqlaFilter(junction="DisjunctionFilter", 
                subfilters=[from_dict(f) for f in dictionary.get('$or')])
        if '$and' in dictionary:
            return SqlaFilter(junction="ConjunctionFilter", 
                subfilters=[from_dict(f) for f in dictionary.get('$and')])   
        
        sqla_filter = {"field" : None, "operator" : None, "argument" : None, "otherfield" : None}            
        sqla_filter["field"], dict_value = next(iter(dictionary.items()))
        sqla_filter["operator"], sqla_filter["argument"] = next(iter(dict_value.items()))
        sqla_filter["otherfield"] = None

        return SqlaFilter(**sqla_filter)

### build search parameters from dictionary
def search_parameters_namespace(dictionary={}):  
    return to_namespace({
        "filters":SqlaFilter.from_dictionary(dictionary.get('filters', {})),                 
        "order_by":[od for od in dictionary.get('order_by', [])] or [], 
        "group_by":[gr for gr in dictionary.get('group_by', [])] or [],
        "limit":dictionary.get('limit'), 
        "offset":dictionary.get('offset')
        })

def sqla_create_operation(model, fieldname, operator, argument, relation=None):
    opfunc = OPERATORS[operator]
    numargs = len(getfullargspec(opfunc).args)
    field = getattr(model, relation or fieldname)
    if numargs == 1:
        return opfunc(field)
    if argument is None:
        msg = ('To compare a value to NULL, use the is_null/is_not_null operators.')
        raise TypeError(msg)
    if numargs == 2:
        return opfunc(field, argument)
    return opfunc(field, argument, fieldname)

def sqla_create_filter(model, filt):     
    if filt.junction == "ConjunctionFilter":
        return and_(*[sqla_create_filter(model, f) for f in filt])
    if filt.junction == "DisjunctionFilter":    
        return or_(*[sqla_create_filter(model, f) for f in filt])
    fieldname = filt.field
    val = filt.argument
    relation = None
    if filt.otherfield:
        val = getattr(model, filt.otherfield)
    return sqla_create_operation(model, fieldname, filt.operator, val, relation)

def sqla_create_query(session, model, search_params, _ignore_order_by=False):
    if isinstance(search_params, dict):
        search_params = search_parameters_namespace(search_params)
    if _ignore_order_by:
        setattr(search_params, order_by, None)
    sqla_query = session_query(session, model)     
    filters = bool(search_params.filters) and [
        sqla_create_filter(model, search_params.filters)] or []
    sqla_query = sqla_query.filter(*filters)
    
    # Order the query.
    if search_params.order_by:
        for order_by in search_params.order_by:
            field = getattr(model, order_by.field, None)
            if field is None:                  
                raise ValueError(f"The order_by field '{order_by.field}' is invalid")  
            direction = getattr(field, order_by.direction, None)
            if direction is None:      
                raise ValueError(f"The order_by direction '{order_by.direction}' is invalid")       
            sqla_query = sqla_query.order_by(direction())

    # Group the query.
    if search_params.group_by:
        for group_by in search_params.group_by:
            field = getattr(model, group_by.field, None)            
            if field is None:      
                raise ValueError(f"The group_by field '{group_by.field}' is invalid")           
            sqla_query = sqla_query.group_by(field)

    # Apply limit and offset to the query.
    if search_params.limit:
        sqla_query = sqla_query.limit(search_params.limit)
    if search_params.offset:
        sqla_query = sqla_query.offset(search_params.offset)

    return sqla_query

#######
        
def catch_integrity_errors(session):
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kw):
            try:
                return func(*args, **kw)
            except (DataError, IntegrityError, ProgrammingError) as exception:
                session.rollback()
                return json({"message":type(exception).__name__}, status=520)
        return wrapped
    return decorator

def _parse_includes(column_names):
    dotted_names, columns = partition(column_names, lambda name: '.' in name)
    relations = defaultdict(list)
    for name in dotted_names:
        relation, field = name.split('.', 1)
        if relation in columns:
            relations[relation].append(field)
    for relation in relations:
        if relation in columns:
            columns.remove(relation)
    return columns, relations

def _parse_excludes(column_names):
    dotted_names, columns = partition(column_names, lambda name: '.' in name)
    relations = defaultdict(list)
    for name in dotted_names:
        relation, field = name.split('.', 1)
        if relation not in columns:
            relations[relation].append(field)
    for column in columns:
        if column in relations:
            del relations[column]
    return columns, relations

def extract_error_messages(exception):
    if hasattr(exception, 'errors'):
        return exception.errors
    if hasattr(exception, 'message'):
        try:
            left, right = str(exception).rsplit(':', 1)
            left_bracket = left.rindex('[')
            right_bracket = right.rindex(']')
        except ValueError as exc:
            return None
        msg = right[:right_bracket].strip(' "')
        fieldname = left[left_bracket + 1:].strip()
        return {fieldname: msg}
    return None

async def run_process(process, **kwargs):
    if not process:
        return None
    if asyncio.iscoroutinefunction(process):
        return await process(**kwargs)
    else:
        return process(**kwargs)

class SQLAView(ModelView):
    db = None
    session = None

    def __init__(self, model=None, collection_name=None, exclude_columns=None,
                 include_columns=None, include_methods=None, results_per_page=10,
                 max_results_per_page=1000, preprocess=None, postprocess=None,
                 primary_key=None, db=None, *args, **kw):

        super(SQLAView, self).__init__(model,collection_name, exclude_columns, include_columns,
                include_methods, results_per_page, max_results_per_page,
                preprocess, postprocess, primary_key, db, *args, **kw)
        
        self.session = kw.pop('session', None)
        validation_exceptions = kw.get('validation_exceptions', None)
        
        serializer = kw.get('serializer', None)
        deserializer = kw.get('deserializer', None)
        
        if db is not None:
            if self.session is None:
                self.session = getattr(self.db, 'session', None)
        
        if exclude_columns is None:
            self.exclude_columns, self.exclude_relations = (None, None)
        else:
            self.exclude_columns, self.exclude_relations = _parse_excludes(
                [self._get_column_name(column) for column in exclude_columns])
        if include_columns is None:
            self.include_columns, self.include_relations = (None, None)
        else:
            self.include_columns, self.include_relations = _parse_includes(
                [self._get_column_name(column) for column in include_columns])
        self.include_methods = include_methods
        
        self.validation_exceptions = tuple(validation_exceptions or ())

        if serializer is None:
            self.serialize = self._inst_to_dict
        else:
            self.serialize = serializer
        if deserializer is None:
            self.deserialize = self._dict_to_inst
            self.validation_exceptions = tuple(list(self.validation_exceptions) + [ValidationError])
        else:
            self.deserialize = deserializer
        
        decorate = lambda name, f: setattr(self, name, f(getattr(self, name)))
        # for method in ['get', 'post', 'patch', 'put', 'delete']:
        for method in ['get', 'post', 'put', 'delete']:
            decorate(method, catch_integrity_errors(self.session))

    def _get_column_name(self, column):
        if hasattr(column, '__clause_element__'):
            clause_element = column.__clause_element__()
            if not isinstance(clause_element, Column):
                msg = ('Column must be a string or a column attribute of SQLAlchemy ORM class')
                raise TypeError(msg)
            model = column.class_
            if model is not self.model:
                msg = ('Cannot specify column of model {0} while creating API'
                       ' for model {1}').format(model.__name__, self.model.__name__)
                raise ValueError(msg)
            return clause_element.key
        return column

    def _add_to_relation(self, query, relationname, toadd=None):
        submodel = get_related_model(self.model, relationname)
        if isinstance(toadd, dict):
            toadd = [toadd]
        for dictionary in toadd or []:
            subinst = get_or_create(self.session, submodel, dictionary)
            try:
                for instance in query:
                    getattr(instance, relationname).append(subinst)
            except AttributeError as exception:
                setattr(instance, relationname, subinst)

    def _remove_from_relation(self, query, relationname, toremove=None):
        submodel = get_related_model(self.model, relationname)
        for dictionary in toremove or []:
            remove = dictionary.pop('__delete__', False)
            if 'id' in dictionary:
                subinst = get_by(self.session, submodel, dictionary['id'])
            else:
                subinst = self.query(submodel).filter_by(**dictionary).first()
            for instance in query:
                getattr(instance, relationname).remove(subinst)
            if remove:
                self.session.delete(subinst)

    def _set_on_relation(self, query, relationname, toset=None):
        submodel = get_related_model(self.model, relationname)
        if isinstance(toset, list):
            value = [get_or_create(self.session, submodel, d) for d in toset]
        else:
            value = get_or_create(self.session, submodel, toset)
        for instance in query:
            setattr(instance, relationname, value)

    def _update_relations(self, query, params):
        relations = get_relations(self.model)
        tochange = frozenset(relations) & frozenset(params)

        for columnname in tochange:
            if (isinstance(params[columnname], dict)
                and any(k in params[columnname] for k in ['add', 'remove'])):

                toadd = params[columnname].get('add', [])
                toremove = params[columnname].get('remove', [])
                self._add_to_relation(query, columnname, toadd=toadd)
                self._remove_from_relation(query, columnname,
                                           toremove=toremove)
            else:
                toset = params[columnname]
                self._set_on_relation(query, columnname, toset=toset)
        return tochange

    def _handle_validation_exception(self, exception):
        self.session.rollback()
        errors = extract_error_messages(exception) or \
            'Could not determine specific validation errors'
        return json(dict(validation_errors=errors), status=520)

    def _compute_results_per_page(self, request):
        """Helper function which returns the number of results per page based
        on the request argument ``results_per_page`` and the server
        configuration parameters :attr:`results_per_page` and
        :attr:`max_results_per_page`.

        """
        try:
            results_per_page = int(request.args.get('results_per_page'))
        except:
            results_per_page = self.results_per_page
        if results_per_page <= 0:
            results_per_page = self.results_per_page
        return min(results_per_page, self.max_results_per_page)

    def _paginated(self, request, instances, deep):
        if isinstance(instances, list):
            num_results = len(instances)
        else:
            num_results = count(self.session, instances)
        results_per_page = self._compute_results_per_page(request)
        if results_per_page > 0:
            page_num = int(request.args.get('page', 1))
            start = (page_num - 1) * results_per_page
            end = min(num_results, start + results_per_page)
            total_pages = int(math.ceil(num_results / results_per_page))
        else:
            page_num = 1
            start = 0
            end = num_results
            total_pages = 1
        objects = [to_dict(x, deep, exclude=self.exclude_columns,
                exclude_relations=self.exclude_relations, include=self.include_columns,
                include_relations=self.include_relations, include_methods=self.include_methods)
                for x in instances[start:end]]
        return dict(page=page_num, objects=objects, total_pages=total_pages, num_results=num_results)


    def _inst_to_dict(self, inst):
        relations = frozenset(get_relations(self.model))
        if self.include_columns is not None:
            cols = frozenset(self.include_columns)
            rels = frozenset(self.include_relations)
            relations &= (cols | rels)
        elif self.exclude_columns is not None:
            relations -= frozenset(self.exclude_columns)
        deep = dict((r, {}) for r in relations)
        return to_dict(inst, deep, exclude=self.exclude_columns,
                       exclude_relations=self.exclude_relations,
                       include=self.include_columns,
                       include_relations=self.include_relations,
                       include_methods=self.include_methods)

    def _dict_to_inst(self, data):
        for field in data:
            if not has_field(self.model, field):
                msg = "Model does not have field '{0}'".format(field)
                raise ValidationError(msg)

        cols = get_columns(self.model)
        relations = get_relations(self.model)

        colkeys = cols.keys()
        paramkeys = data.keys()
        props = set(colkeys).intersection(paramkeys).difference(relations)

        data = strings_to_dates(self.model, data)

        modelargs = dict([(i, data[i]) for i in props])
        instance = self.model(**modelargs)

        for col in set(relations).intersection(paramkeys):
            submodel = get_related_model(self.model, col)

            if type(data[col]) == list:
                for subparams in data[col]:
                    subinst = get_or_create(self.session, submodel, subparams)
                    try:
                        getattr(instance, col).append(subinst)
                    except AttributeError:
                        attribute = getattr(instance, col)
                        attribute[subinst.key] = subinst.value
            else:
                if data[col] is not None:
                    subinst = get_or_create(self.session, submodel, data[col])
                    setattr(instance, col, subinst)

        return instance

    def _instid_to_dict(self, instid):
        inst = get_by(self.session, self.model, instid, self.primary_key)
        if inst is None:
            return json(dict(message='No result found'), status=520)
        return self._inst_to_dict(inst)


    async def _search(self, request):
        try:
            search_params = json_loads(request.args.get('q', '{}'))
        except (TypeError, ValueError, OverflowError) as exception:
            return json(dict(message='Unable to decode data'), status=520)

        try:
            for preprocess in self.preprocess['GET_MANY']:     
                resp = await run_process(process=preprocess, request=request,
                        search_params=search_params, Model=self.model)
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except ProcessingException as exception:
            return response_exception(exception)

        try:
            result = sqla_create_query(self.session, self.model, search_params)
        except NoResultFound:
            return json(dict(message='No result found'), status=520)
        except MultipleResultsFound:
            return json(dict(message='Multiple results found'), status=520)
        except Exception as exception:
            return json(dict(message='Unable to construct query'), status=520)

        relations = frozenset(get_relations(self.model))
        if self.include_columns is not None:
            cols = frozenset(self.include_columns)
            rels = frozenset(self.include_relations)
            relations &= (cols | rels)
        elif self.exclude_columns is not None:
            relations -= frozenset(self.exclude_columns)
        deep = dict((r, {}) for r in relations)

        if isinstance(result, Query):
            result = self._paginated(request, result, deep)
        else:
            # primary_key = self.primary_key or primary_key_name(result)
            result = to_dict(result, deep, exclude=self.exclude_columns,
                             exclude_relations=self.exclude_relations,
                             include=self.include_columns,
                             include_relations=self.include_relations,
                             include_methods=self.include_methods)
        try:
            headers = {}
            for postprocess in self.postprocess['GET_MANY']:
                resp = await run_process(process=postprocess, request=request, result=result, 
                        search_params=search_params, Model=self.model, headers=headers)
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except ProcessingException as exception:
            return response_exception(exception)
        return json(result, headers=headers, status=200)

    async def get(self, request, instid=None, relationname=None, relationinstid=None):
        if instid is None:
            return await self._search(request)

        try:
            for preprocess in self.preprocess['GET_SINGLE']:           
                resp = await run_process(process=preprocess, request=request, 
                        instance_id=instid, Model=self.model)
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp                
                if resp is not None:
                    instid = resp
                    
        except ProcessingException as exception:
            return response_exception(exception)

        instance = get_by(self.session, self.model, instid, self.primary_key)

        if instance is None:
            return json(dict(message='No result found'),status=520)
        
        if relationname is None:
            result = self.serialize(instance)
        else:
            related_value = getattr(instance, relationname)
            related_model = get_related_model(self.model, relationname)
            relations = frozenset(get_relations(related_model))
            deep = dict((r, {}) for r in relations)
            if relationinstid is not None:
                related_value_instance = get_by(self.session, related_model, relationinstid)
                if related_value_instance is None:
                    return json(dict(message='No result found'),status=520)
                result = to_dict(related_value_instance, deep)
            else:
                if is_like_list(instance, relationname):
                    result = self._paginated(list(related_value), deep)
                else:
                    result = to_dict(related_value, deep)
        if result is None:
            return json(dict(message='No result found'),status=520)

        try:
            headers = {}
            for postprocess in self.postprocess['GET_SINGLE']:
                resp = await run_process(process=postprocess,request=request, instance_id=instid, 
                    result=result, Model=self.model, headers=headers)
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except ProcessingException as exception:
            return response_exception(exception)

        return json(result, headers=headers, status=200)

    async def _delete_many(self, request):
        try:
            search_params = json_loads(request.args.get('q', '{}'))
        except (TypeError, ValueError, OverflowError) as exception:
            return json(dict(message='Unable to decode search query'), status=520)

        try:
            for preprocess in self.preprocess['DELETE_MANY']:
                resp = await run_process(process=preprocess, request=request, 
                    search_params=search_params, Model=self.model)
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except ProcessingException as exception:
            return response_exception(exception)

        try:
            result = sqla_create_query(self.session, self.model, search_params,
                            _ignore_order_by=True)
        except NoResultFound:
            return json(dict(message='No result found'), status=520)
        except MultipleResultsFound:
            return json(dict(message='Multiple results found'), status=520)
        except Exception as exception:
            return json(dict(message='Unable to construct query'), status=520)

        if isinstance(result, Query):
            num_deleted = result.delete(synchronize_session=False)
        else:
            self.session.delete(result)
            num_deleted = 1
        self.session.commit()
        result = dict(num_deleted=num_deleted)

        try:
            headers = {}
            for postprocess in self.postprocess['DELETE_MANY']:
                resp = await run_process(process=postprocess, request=request, result=result, 
                    search_params=search_params, Model=self.model, headers=headers)
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except ProcessingException as exception:
            return response_exception(exception)

        return (json(result, headers=headers, status=200)) if num_deleted > 0 else json({}, headers=headers, status=520)

    async def delete(self, request, instid=None, relationname=None, relationinstid=None):
        if instid is None:
            return await self._delete_many(request)
        was_deleted = False

        try:
            for preprocess in self.preprocess['DELETE_SINGLE']:
                resp = await run_process(process=preprocess, request=request, instance_id=instid,
                    relation_name=relationname, relation_instance_id=relationinstid, Model=self.model)
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
                if resp is not None:
                    instid = resp
        except ProcessingException as exception:
            return response_exception(exception)

        inst = get_by(self.session, self.model, instid, self.primary_key)
        if relationname:
            if not relationinstid:
                msg = ('Cannot DELETE entire "{0}" relation').format(relationname)
                return json(dict(message=msg), status=520)
            relation = getattr(inst, relationname)
            related_model = get_related_model(self.model, relationname)
            relation_instance = get_by(self.session, related_model, relationinstid)
            relation.remove(relation_instance)
            was_deleted = len(self.session.dirty) > 0
        elif inst is not None:
            self.session.delete(inst)
            was_deleted = len(self.session.deleted) > 0
        self.session.commit()

        try:
            headers = {}
            for postprocess in self.postprocess['DELETE_SINGLE']:
                resp = await run_process(process=postprocess, request=request, instance_id=instid, 
                        was_deleted=was_deleted, Model=self.model, headers=headers) 
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except ProcessingException as exception:
            return response_exception(exception)

        return json({}, headers=headers, status=200) if was_deleted else json({}, headers=headers, status=520)

    async def post(self, request):
        content_type = request.headers.get('Content-Type', "")
        content_is_json = content_type.startswith('application/json')

        if not content_is_json:
            msg = 'Request must have "Content-Type: application/json" header'
            return json(dict(message=msg),status=520)

        try:
            data = request.json or {}
        except (ServerError, TypeError, ValueError, OverflowError) as exception:
            return json(dict(message='Unable to decode data'),status=520)

        try:
            for preprocess in self.preprocess['POST']:
                resp = await run_process(process=preprocess, request=request, 
                        data=data, Model=self.model)
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except ProcessingException as exception:
            return response_exception(exception)

        try:
            instance = self.deserialize(data)
            self.session.add(instance)
            self.session.commit()
            result = self.serialize(instance)
        except self.validation_exceptions as exception:
            return self._handle_validation_exception(exception)
        pk_name = self.primary_key or primary_key_name(instance)
        primary_key = result[pk_name]
        try:
            primary_key = str(primary_key)
        except UnicodeEncodeError:
            print("TODO: url_quote_plus() implement in sanic_restapi views")
            primary_key = primary_key.encode('utf-8')
        url = '{0}/{1}'.format(request.url, primary_key)
        headers = dict(Location=url)

        try:
            headers = {}
            for postprocess in self.postprocess['POST']:
                resp = await run_process(process=postprocess, request=request, result=result, 
                        Model=self.model, headers=headers)
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except ProcessingException as exception:
            return response_exception(exception)
        return json(result,headers=headers, status=201)

    async def put(self, request, instid=None, relationname=None, relationinstid=None):
        content_type = request.headers.get('Content-Type', "")
        content_is_json = content_type.startswith('application/json')
        if not content_is_json:
            msg = 'Request must have "Content-Type: application/json" header'
            return json(dict(message=msg),status=520)
        try:
            data = request.json or {}
        except (ServerError, TypeError, ValueError, OverflowError) as exception:
            return json(dict(message='Unable to decode data'),status=520)

        putmany = instid is None
        if putmany:
            search_params = data.pop('q', {})
            try:
                for preprocess in self.preprocess['PUT_MANY']:
                    resp = await run_process(process=preprocess, request=request, 
                            search_params=search_params, data=data, Model=self.model)
                    if (resp is not None) and isinstance(resp, HTTPResponse):
                        return resp
            except ProcessingException as exception:
                return response_exception(exception)

        else:
            for preprocess in self.preprocess['PUT_SINGLE']:
                try:
                    resp = await run_process(process=preprocess, request=request, 
                        instance_id=instid, data=data, Model=self.model)
                    if (resp is not None) and isinstance(resp, HTTPResponse):
                        return resp
                    if resp is not None:
                        instid = resp
                except ProcessingException as exception:
                    return response_exception(exception)

        for field in data:
            if not has_field(self.model, field):
                msg = "Model does not have field '{0}'".format(field)
                return json(dict(message=msg),status=520)

        if putmany:
            try:
                query = sqla_create_query(self.session, self.model, search_params)
            except Exception as exception:
                return json(dict(message='Unable to construct query'),status=520)
        else:
            query = query_by_primary_key(self.session, self.model, instid,
                                         self.primary_key)
            if query.count() == 0:
                return json(dict(message='No result found'), status=520)
            assert query.count() == 1, 'Multiple rows with same ID'
        try:
            relations = self._update_relations(query, data)
        except self.validation_exceptions as exception:
            #current_app.logger.exception(str(exception))
            return self._handle_validation_exception(exception)
        field_list = frozenset(data) ^ relations

        data = dict((field, data[field]) for field in field_list)
        data = strings_to_dates(self.model, data)
        try:
            num_modified = 0
            if data:
                for item in query.all():
                    for field, value in data.items():
                        setattr(item, field, value)
                    num_modified += 1
            self.session.commit()
        except self.validation_exceptions as exception:
            return self._handle_validation_exception(exception)

        headers = {}
        if putmany:
            result = dict(num_modified=num_modified)
            try:
                for postprocess in self.postprocess['PUT_MANY']:
                    resp = await run_process(process=postprocess, request=request, query=query, 
                        result=result, search_params=search_params, Model=self.model, headers=headers)                        
                    if (resp is not None) and isinstance(resp, HTTPResponse):
                        return resp
            except ProcessingException as exception:
                return response_exception(exception)

        else:
            result = self._instid_to_dict(instid)
            try:
                for postprocess in self.postprocess['PUT_SINGLE']:
                    resp = await run_process(process=postprocess,request=request, 
                            instance_id=instid, result=result, Model=self.model, headers=headers)
                    if (resp is not None) and isinstance(resp, HTTPResponse):
                        return resp
            except ProcessingException as exception:
                return response_exception(exception)

        return json(result, headers=headers, status=200)

    async def _put_many(self, request):
        pass