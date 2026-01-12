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

from .core import ModelView
from ..exception import (ProcessingException, ValidationError, response_exception)
######
from inspect import getfullargspec

from sqlalchemy import (and_,or_)
from ..helpers import to_namespace

from ..constant import OPERATORS
        
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
            
        if include_columns is None:
            self.include_columns, self.include_relations = (None, None)
            
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


    # async def _search(self, request):
    # async def get(self, request, instid=None, relationname=None, relationinstid=None):
        
    # async def _delete_many(self, request):

    # async def delete(self, request, instid=None, relationname=None, relationinstid=None):
        
    # async def post(self, request):

    # async def put(self, request, instid=None, relationname=None, relationinstid=None):

    async def _put_many(self, request):
        pass