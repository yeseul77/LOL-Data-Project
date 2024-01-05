from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from router.router import router2

app = FastAPI()

# CORS설정
origins = [
    'http://localhost:63342/'
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # origins ,오리진에대한 포트만열어주겠다,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"]
)


class PhoneList(BaseModel):
    name: str = ''
    phone: str = ''


@app.get("/")
def root():
    return {"hello": "world"}


@app.get("/test")
def test(a: int, b: int):
    c = a + b
    return c


@app.get("/submit_data", response_model=PhoneList)
# phoneList를 response
def print_name_num(
        name: str,
        phone: str
) -> PhoneList:
    # ->PhoneList:
    a = f'{name}과{phone}잘받았습니다.'
    # return a#return jSON형태로받아와야함
    return PhoneList(
        name=name,
        phone=phone)


# app.include_router(router, tags=['test'])
app.include_router(router2, tags=['test2'])
# 서버올리는 명령어
# uviconrn main:app --reload
# [/docs#]
