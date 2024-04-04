import asyncio
import json
from typing import Set, Dict, List, Any
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body
from fastapi.encoders import jsonable_encoder
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from datetime import datetime
from pydantic import BaseModel, field_validator
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

# FastAPI app setup
app = FastAPI()
# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("parking", Integer),
    Column("parking_latitude", Float),
    Column("parking_longitude", Float),
    Column("timestamp", DateTime),
)
SessionLocal = sessionmaker(bind=engine)


# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class ParkingData(BaseModel):
    parking: int
    parking_gps: GpsData


class AgentData(BaseModel):
    accelerometer: AccelerometerData
    gps: GpsData
    parking: ParkingData
    timestamp: datetime

    @classmethod
    @field_validator('timestamp', mode='before')
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError("Invalid timestamp format. Expected ISO 8601 format(YYYY-MM-DDTHH:MM:SSZ).")


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData


# Database model
class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    parking: int
    parking_latitude: float
    parking_longitude: float
    timestamp: datetime


# FastAPI app setup
app = FastAPI()

# WebSocket subscriptions
subscriptions: Set[WebSocket] = set()


# FastAPI WebSocket endpoint
@app.websocket("/ws/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    subscriptions.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions.remove(websocket)

# Function to send data to subscribed users
async def send_data_to_subscribers(data):
    for websocket in subscriptions:
        await websocket.send_json(json.dumps(data))


def setglobdata(data : dict):
    globdata = list()
    for i in range(len(data)):
        istr = str(i)
        globdata.append(ProcessedAgentDataInDB(id = i, road_state = data[istr][0],
        x = data[istr][1], y = data[istr][2], z = data[istr][3], 
        latitude = data[istr][4], longitude = data[istr][5],
        parking = data[istr][6], parking_latitude = data[istr][7], parking_longitude = data[istr][8],
        timestamp = datetime.fromisoformat(data[istr][9])))
        # datetime.strptime(data[istr][9], "%Y-%m-%d %H:%M:%S.%f")))
    return globdata


def setwdata(wdata : dict, data : list[ProcessedAgentData]):
    offset = len(wdata)
    for i in range(len(data)):
        wdata.update({str(i + offset): [data[i].road_state, data[i].agent_data.accelerometer.x, data[i].agent_data.accelerometer.y, data[i].agent_data.accelerometer.z,
        data[i].agent_data.gps.latitude, data[i].agent_data.gps.longitude, data[i].agent_data.parking.parking, 
        data[i].agent_data.parking.parking_gps.latitude, data[i].agent_data.parking.parking_gps.longitude, str(data[i].agent_data.timestamp)]})
    return wdata


adress = "database.json"


# FastAPI CRUDL endpoints
@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    try:
        with open(adress, "r") as f:
            wdata = json.loads(json.load(f))
    except:
        wdata = {}
    wdata = setwdata(wdata, data)
    # Insert data to database
    with open(adress, "w") as f:
        json.dump(json.dumps(wdata), f, indent=4)
    # Send data to subscribers
    send_data_to_subscribers(data)


@app.get(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB
)
def read_processed_agent_data(processed_agent_data_id: int):
    # Get data by id
    with open(adress, "r") as f:
        wdata = json.loads(json.load(f))
    if processed_agent_data_id >= len(wdata):
        raise HTTPException(status_code=201, detail="Number bigger than lenght of data")
    return setglobdata(wdata)[processed_agent_data_id]


@app.get(
    "/processed_agent_data/",
    response_model=list[ProcessedAgentDataInDB]
)
def list_processed_agent_data():
    # Get list of data
    with open(adress, "r") as f:
        wdata = json.loads(json.load(f))
    return setglobdata(wdata)


@app.put(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB
)
def update_processed_agent_data(
    processed_agent_data_id: int,
    data: ProcessedAgentData
):
    # Update data
    with open(adress, "r") as f:
        wdata = json.loads(json.load(f))
    if processed_agent_data_id >= len(wdata):
        raise HTTPException(status_code=201, detail="Number bigger than lenght of data")
    wdata[str(processed_agent_data_id)] = [data.road_state, data.agent_data.accelerometer.x, data.agent_data.accelerometer.y, data.agent_data.accelerometer.z,
        data.agent_data.gps.latitude, data.agent_data.gps.longitude, data.agent_data.parking.parking, 
        data.agent_data.parking.parking_gps.latitude, data.agent_data.parking.parking_gps.longitude, str(data.agent_data.timestamp)]
    with open(adress, "w") as f:
        json.dump(json.dumps(wdata), f, indent=4)
    return setglobdata(wdata)[processed_agent_data_id]


@app.delete(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB
)
def delete_processed_agent_data(processed_agent_data_id: int):
    # Delete by id
    with open(adress, "r") as f:
        wdata = json.loads(json.load(f))
    if processed_agent_data_id >= len(wdata):
        raise HTTPException(status_code=201, detail="Number bigger than lenght of data")
    temp = wdata.pop(str(processed_agent_data_id))
    for i in range(len(wdata)):
        if wdata.get(str(i)) == None:
            wdata.update({str(i): wdata.pop(str(i+1))})
    with open(adress, "w") as f:
        json.dump(json.dumps(wdata), f, indent=4)
    return ProcessedAgentDataInDB(id = processed_agent_data_id,
    road_state = temp[0],x = temp[1], y = temp[2], z = temp[3], 
    latitude = temp[4], longitude = temp[5], parking = temp[6], 
    parking_latitude = temp[7], parking_longitude = temp[8],
    timestamp = datetime.fromisoformat(temp[9]))
    # datetime.strptime(temp[9], "%Y-%m-%d %H:%M:%S.%f"))


@app.delete(
    "/processed_agent_data/",
    response_model=list[ProcessedAgentDataInDB]
)
def clear_processed_agent_data():
    # Delete all
    with open(adress, "r") as f:
        wdata = json.loads(json.load(f))
    with open(adress, "w") as f:
        json.dump(json.dumps({}), f, indent=4)
    return setglobdata(wdata)


if __name__ == "__main__":
    import uvicorn
    
    
    uvicorn.run(app, host="127.0.0.1", port=8000)