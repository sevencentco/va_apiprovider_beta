# SQLALCHEMY_DATABASE_URI = "postgresql+asyncpg://user:pass@localhost/mydb"

# @app.delete("/users/<user_id:int>")
# async def mandas(request, user_id):
#     user = await db.session.get(User, user_id)
#     if not user:
#         return json({"ok": False}, status=404)

#     await db.session.delete(user)
#     return json({"ok": True})


# ###
# await session.get(Model, id)
# await session.execute(stmt)
# await session.scalars(stmt)
# await session.scalar(stmt)

# await session.flush()
# await session.commit()
# await session.rollback()
# await session.refresh(obj)
# await session.close()
