import redis

class RedisDB:
    def __init__(self, app=None, host="127.0.0.1", port=6379, db=0, **kwargs):
        self.client = None
        self.default_config = {"REDIS_HOST": host,"REDIS_PORT": port,"REDIS_DB": db,}
        self.extra_params = kwargs

        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        host = app.config.get("REDIS_HOST", self.default_config["REDIS_HOST"])
        port = app.config.get("REDIS_PORT", self.default_config["REDIS_PORT"])
        db   = app.config.get("REDIS_DB", self.default_config["REDIS_DB"])

        self.client = redis.Redis( host=host, port=port, db=db,**self.extra_params)

        if not hasattr(app, "ctx"):
            app.ctx = type("C", (), {})()
        app.ctx.redis = self.client
        if not hasattr(app.ctx, "extensions") or app.ctx.extensions is None:
            app.ctx.extensions = {}
        app.ctx.extensions["redis"] = self.client

        return self.client
    
    def __getattr__(self, name):
        """Forward tất cả method/attr cho redis client"""
        return getattr(self.client, name)
    

