
@app.delete("/users/<user_id:int>")
async def mandas(request, user_id):
    user = await db.session.get(User, user_id)
    if not user:
        return json({"ok": False}, status=404)

    await db.session.delete(user)
    return json({"ok": True})
