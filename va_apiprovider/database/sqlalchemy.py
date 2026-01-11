import asyncio
from threading import Lock
from sqlalchemy import create_engine
from sqlalchemy.orm import (
    declarative_base, scoped_session, sessionmaker
)

class DatabaseAlchemy:
    def __init__(self, app=None, uri=None):
        self.app = app
        self._engine = None
        self._engine_lock = Lock()

        self.uri = uri
        self.session = None
        self.Model = declarative_base()

        if app is not None:
            self.init_app(app)

    # -----------------------
    # Engine + Session
    # -----------------------
    @property
    def engine(self):
        if self._engine is None:
            with self._engine_lock:
                if self._engine is None:
                    self._engine = create_engine(self.uri)
        return self._engine

    @property
    def metadata(self):
        return self.Model.metadata

    def _make_scoped_session(self):
        def _scopefunc():
            try:
                return asyncio.current_task()
            except RuntimeError:
                print("You are calling a synchronous function — use async to run it properly")
                return None        
        Session = scoped_session(sessionmaker(bind=self.engine),scopefunc=asyncio.current_task,)
        self.Model.query = Session.query_property()
        return Session

    # -----------------------
    # Init app
    # -----------------------
    def init_app(self, app=None, uri=None):
        uri_ = (uri or self.uri or app.config.get("SQLALCHEMY_DATABASE_URI") or "sqlite:///:memory:")

        self.uri = uri_
        self.session = self._make_scoped_session()

        if not hasattr(app, "ctx"):
            app.ctx = type("C", (), {})()

        if not hasattr(app.ctx, "extensions"):
            app.ctx.extensions = {}

        app.ctx.extensions["sqlalchemy"] = self

        @app.middleware("response")
        async def shutdown_session(request, response):
            try:
                if app.config.get("SQLALCHEMY_COMMIT_ON_RESPONSE"):
                    self.session.commit()
            except:
                self.session.rollback()
                raise
            finally:
                self.session.remove()

    # -----------------------
    # Helpers
    # -----------------------
    def create_all(self):
        self.metadata.create_all(self.engine)

    def drop_all(self):
        self.metadata.drop_all(self.engine)

    def __repr__(self):
        return f"<SQLAlchemy engine={self.uri!r}>"
