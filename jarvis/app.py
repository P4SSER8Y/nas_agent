from fastapi import FastAPI

__all__ = ["app"]

app = FastAPI()

@app.get("/")
def main():
    return "Hello World"
