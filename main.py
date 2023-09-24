from sqlalchemy import create_engine, Engine, text
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.engine import Connection, Result, CursorResult, Row

engine: Engine = create_engine('sqlite://', echo=True)
print(engine)
connection: Connection = engine.connect()
print(connection)
stmt: TextClause = text('SELECT \'hello world\' as greeting')
result: Result = connection.execute(stmt)
print(result)
# row: Row = result.first()
# print(type(row), row, row[0], row.greeting, row._mapping['greeting'])
# for row in result:
#     print(row)
for greeting in result:
    print(f'greeting: {greeting}')
for greeting in result.scalars():
    print(f'greeting: {greeting}')
engine.begin()
