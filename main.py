import logging
import os

import uvicorn


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    uvicorn.run("app.api:app", host="127.0.0.1", port=8000, reload=True)
