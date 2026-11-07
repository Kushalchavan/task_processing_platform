from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def root():
    return {"Hello": "world"}


@app.get("/items/{item_id}")
def get_item(item_id: int, q: str | None = None):
    return {"item_id": {item_id}, "q": {q}}