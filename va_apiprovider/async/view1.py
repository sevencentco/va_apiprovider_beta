import asyncio
from .core import ModelView
from .exception import (ProcessingException, ValidationError, response_exception)

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