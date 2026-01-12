from contextvars import ContextVar
from sqlalchemy.ext.asyncio import ( create_async_engine, AsyncSession, async_sessionmaker, )
from sqlalchemy.orm import declarative_base

_current_session: ContextVar[AsyncSession | None] = ContextVar(
    "current_db_session", default=None
)


class DatabaseAlchemy:
    def __init__(self, app=None, uri=None):
        self.app = app
        self.uri = uri

        self._engine = None
        self._sessionmaker = None

        self.Model = declarative_base()

        if app is not None:
            self.init_app(app)

    # -----------------------
    # Engine
    # -----------------------
    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_async_engine( self.uri, echo=False, future=True, )
        return self._engine

    @property
    def metadata(self):
        return self.Model.metadata

    # -----------------------
    # Session factory
    # -----------------------
    def _make_sessionmaker(self):
        return async_sessionmaker( bind=self.engine, class_=AsyncSession, expire_on_commit=False, )

    @property
    def session(self) -> AsyncSession:
        session = _current_session.get()
        if session is None:
            raise RuntimeError("No active DB session. Did you forget to enable Sanic middleware?")
        return session

    # -----------------------
    # Init app (Sanic)
    # -----------------------
    def init_app(self, app=None, uri=None):
        app = app or self.app

        self.uri = (
            uri or self.uri or app.config.get("SQLALCHEMY_DATABASE_URI")
            or "sqlite+aiosqlite:///:memory:"
        )

        if not hasattr(app, "ctx"):
            app.ctx = type("C", (), {})()

        if not hasattr(app.ctx, "extensions"):
            app.ctx.extensions = {}

        app.ctx.extensions["sqlalchemy"] = self

        # -------- request: open session --------
        @app.middleware("request")
        async def open_db_session(request):
            if self._sessionmaker is None:
                self._sessionmaker = self._make_sessionmaker()

            session = self._sessionmaker()
            token = _current_session.set(session)
            request.ctx._db_session_token = token

        # -------- response: close session --------
        @app.middleware("response")
        async def close_db_session(request, response):
            token = getattr(request.ctx, "_db_session_token", None)
            if not token:
                return response

            session = _current_session.get()

            try:
                if app.config.get("SQLALCHEMY_COMMIT_ON_RESPONSE"):
                    await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()
                _current_session.reset(token)

            return response

    # -----------------------
    # Helpers
    # -----------------------
    async def create_all(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(self.metadata.create_all)

    async def drop_all(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(self.metadata.drop_all)

    def __repr__(self):
        return f"<AsyncSQLAlchemy engine={self.uri!r}>"
