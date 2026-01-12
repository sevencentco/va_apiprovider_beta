import asyncio
from sqlalchemy.ext.asyncio import ( create_async_engine, AsyncSession, async_sessionmaker, )
from sqlalchemy.orm import declarative_base
from contextlib import asynccontextmanager


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
    # Engine + Session
    # -----------------------
    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_async_engine(self.uri,echo=False,future=True,)
        return self._engine

    @property
    def metadata(self):
        return self.Model.metadata

    def _make_sessionmaker(self):
        return async_sessionmaker(bind=self.engine,class_=AsyncSession,expire_on_commit=False,)

    @asynccontextmanager
    async def session(self) -> AsyncSession:
        """
        Usage:
        async with db.session() as session:
            ...
        """
        if self._sessionmaker is None:
            self._sessionmaker = self._make_sessionmaker()

        async with self._sessionmaker() as session:
            try:
                yield session
                if self.app and self.app.config.get("SQLALCHEMY_COMMIT_ON_RESPONSE"):
                    await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    # -----------------------
    # Init app
    # -----------------------
    def init_app(self, app=None, uri=None):
        app = app or self.app

        self.uri = ( uri or self.uri or app.config.get("SQLALCHEMY_DATABASE_URI")
            or "sqlite+aiosqlite:///:memory:" )

        if not hasattr(app, "ctx"):
            app.ctx = type("C", (), {})()

        if not hasattr(app.ctx, "extensions"):
            app.ctx.extensions = {}

        app.ctx.extensions["sqlalchemy"] = self

        @app.middleware("response")
        async def shutdown_session(request, response):
            # Session lifecycle đã xử lý bằng context manager
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
