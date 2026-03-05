from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def fetch_one(db: AsyncSession, query: str, params: dict | None = None):
    result = await db.execute(text(query), params or {})
    return result.fetchone()


async def fetch_all(db: AsyncSession, query: str, params: dict | None = None):
    result = await db.execute(text(query), params or {})
    return result.fetchall()


async def execute(db: AsyncSession, query: str, params: dict | None = None):
    await db.execute(text(query), params or {})
    await db.commit()


async def execute_returning(db: AsyncSession, query: str, params: dict | None = None):
    result = await db.execute(text(query), params or {})
    await db.commit()
    return result.fetchone()