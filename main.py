import uvicorn
from fastapi import FastAPI, Body

from orm import _connection_check, _database_list, _table_list, _target_user_list, _check_authority, \
    _authority_list, _add_authority, _remove_authority

app = FastAPI()


@app.post('/connection_check')
async def connection_check(_json=Body(...)):
    code, result = _connection_check(_json)
    return {'code': code, 'result': result}


@app.post('/target_user_list')
async def target_user_list(_json=Body(...)):
    code, result = _target_user_list(_json)
    return {'code': code, 'result': result}


@app.post('/table_list')
async def table_list(_json=Body(...)):
    code, result = _table_list(_json)
    return {'code': code, 'result': result}


@app.post('/check_authority')
async def check_authority(_json=Body(...)):
    code, result = _check_authority(_json)
    return {'code': code, 'result': result}


@app.post('/authority_list')
async def authority_list(_json=Body(...)):
    try:
        code, result = _authority_list(_json)
        return {'code': code, 'result': result}
    except:
        return {'code': 2, 'result': 'Failed to retrieve authorization list'}


@app.post('/add_authority')
async def add_authority(_json=Body(...)):
    code, result = _add_authority(_json)
    return {'code': code, 'result': result}


@app.post('/remove_authority')
async def remove_authority(_json=Body(...)):
    code, result = _remove_authority(_json)
    return {'code': code, 'result': result}


if __name__ == "__main__":
    uvicorn.run('main:app', host="127.0.0.1", port=8000)
