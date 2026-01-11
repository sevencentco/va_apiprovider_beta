from collections import defaultdict
# from types import SimpleNamespace
from collections import namedtuple
from sanic import Blueprint, response

from .core import (RestInfo,ModelView)
from .exception import IllegalArgumentError
from .constant import (READONLY_METHODS, BLUEPRINTNAME_FORMAT, APINAME_FORMAT)
from .helpers import to_namespace

from collections import defaultdict

def next_blueprint_name(blueprints, basename):        
    existing = [name for name in blueprints if name.startswith(basename)]
    if not existing: ### if this is the first one...
        next_number = 0
    else:
        b = basename
        existing_numbers = [int(n.partition(b)[-1]) for n in existing]
        next_number = max(existing_numbers) + 1
    return BLUEPRINTNAME_FORMAT.format(basename, next_number)

def api_provider(name="restapi", app=None, **kw):   
    _name = name
    _app = app
    _view_cls = None
    _apis_to_create = defaultdict(list)
    _created_apis_for = {}
    if _app is not None:
        init_app(_app, **kw)           
            
    def init_app(app, view_cls=ModelView, preprocess=None, postprocess=None, db=None, *args, **kw):
        nonlocal _name, _app, _view_cls, _apis_to_create, _created_apis_for
        if not hasattr(app, "ctx"):
            app.ctx = type("C", (), {})()
        if not hasattr(app.ctx, "extensions") or app.ctx.extensions is None:
            app.ctx.extensions = {}
            
        if _name in app.ctx.extensions:
            raise ValueError(_name + ' has already been initialized on'
                             ' this application: {0}'.format(app))
        app.ctx.extensions[_name] = RestInfo(db, preprocess or {}, postprocess or {})
        
        if app is not None:
            _app = app
            
        if view_cls is not None:
            _view_cls = view_cls

        to_create = _apis_to_create.pop(app, []) + _apis_to_create.pop(None, [])
        
        for args, kw in to_create:
            blueprint = create_api_blueprint(app=app, *args, **kw)
            app.blueprint(blueprint)
            
    def create_api_blueprint(model=None, collection_name=None, app=None, methods=READONLY_METHODS,
                             url_prefix='/api', exclude_columns=None,
                             include_columns=None, include_methods=None,
                             results_per_page=10, max_results_per_page=100,
                             preprocess=None, postprocess=None, primary_key=None, *args, **kw):
        nonlocal _name, _app, _view_cls, _apis_to_create, _created_apis_for
        if collection_name is None:
            msg = ('collection_name is not valid.')
            raise IllegalArgumentError(msg)
            
        if exclude_columns is not None and include_columns is not None:
            msg = ('Cannot simultaneously specify both include columns and'
                   ' exclude columns.')
            raise IllegalArgumentError(msg)
        
        if app is None:
            app = _app
            
        restapi_ext = app.ctx.extensions[_name]
        
        methods = frozenset((m.upper() for m in methods))
        no_instance_methods = methods & frozenset(('POST', ))
        instance_methods = methods & frozenset(('GET', 'PATCH', 'DELETE', 'PUT'))
        possibly_empty_instance_methods = methods & frozenset(('GET', ))
        
        # the base URL of the endpoints on which requests will be made
        collection_endpoint = '/{0}'.format(collection_name)
        
        apiname = APINAME_FORMAT.format(collection_name)
        
        preprocessors_ = defaultdict(list)
        postprocessors_ = defaultdict(list)
        preprocessors_.update(preprocess or {})
        postprocessors_.update(postprocess or {})
        
        api_view = _view_cls.as_view(model=model, collection_name=collection_name,exclude_columns=exclude_columns,\
                include_columns=include_columns, include_methods=include_methods,\
                results_per_page=results_per_page, max_results_per_page=max_results_per_page, \
                preprocess=preprocessors_, postprocess=postprocessors_, primary_key=primary_key,\
                db=restapi_ext.db)
                               
        bp_name = next_blueprint_name(app.blueprints, apiname)        
        bp_route_name = bp_name + "_nim" #### no_instance_methods
        blueprint = Blueprint(bp_name, url_prefix=url_prefix)
        blueprint.add_route(handler=api_view, uri=collection_endpoint,
                methods=no_instance_methods, name=bp_route_name,)
        
        #DELETE, GET, PUT    
        bp_route_name = bp_name + "_im" #### instance_methods
        instance_endpoint = '{0}/<instid>'.format(collection_endpoint)
        blueprint.add_route(handler=api_view, uri=instance_endpoint,
                methods=instance_methods, name=bp_route_name,)
        
        return blueprint
    
    def create_api(*args, **kw):
        nonlocal _name, _app, _view_cls, _apis_to_create, _created_apis_for
        if 'app' in kw:
            if _app is not None:
                msg = ('Cannot provide a application in the APIProvider'
                       ' constructor and in create_api(); must choose exactly one')
                raise IllegalArgumentError(msg)
            app = kw.pop('app')
            if _name in app.ctx.extensions:
                blueprint = create_api_blueprint(app=app, *args, **kw)
                app.blueprint(blueprint)
            else:
                _apis_to_create[app].append((args, kw))
        else:
            if _app is not None:
                app = _app
                blueprint = create_api_blueprint(app=app, *args, **kw)
                app.blueprint(blueprint)
            else:
                _apis_to_create[None].append((args, kw))

    return to_namespace({
        "init_app" : init_app, 
        "create_api" : create_api, 
        "create_api_blueprint" : create_api_blueprint,
        "state" : {
            "name" : lambda: _name, "app" : lambda: _app, 
            "queued" : lambda: _apis_to_create,
        }, 
    })
