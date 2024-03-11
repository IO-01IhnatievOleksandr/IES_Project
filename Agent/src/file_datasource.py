from csv import reader
from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.parking import Parking
from domain.aggregated_data import AggregatedData
import config

class FileDatasource:
    global work
    work = False
    global r1, r2, r3, r4, r5
    global accname, coordname, dtname, pgpsname, pname
    def __init__(self, accelerometer_filename: str, gps_filename: str, datetime_filename: str, parkinggps_filename: str, parking_filename: str) -> None:
        global accname, coordname, dtname, pgpsname, pname
        accname = accelerometer_filename
        coordname = gps_filename
        dtname = datetime_filename
        pname = parking_filename
        pgpsname = parkinggps_filename

    def read(self) -> AggregatedData:
        """Метод повертає дані отримані з датчиків"""
        global work, gcount, r1, r2, r3, r4, r5
        if work:
            if r2.line_num == 126:
                work = False
                return 0
            else:
                t1 = r1.__next__()
                t2 = r2.__next__()
                t3 = r3.__next__()
                t4 = r4.__next__()
                t5 = r5.__next__()
                return AggregatedData(
                Accelerometer(t1[0], t1[1], t1[2]),
                Gps(t2[0], t2[1]),
                Parking(t5[0], Gps(t4[0], t4[1])),
                datetime.strptime(t3[0], "%Y-%m-%d %H:%M:%S.%f")
        )
    
    def startReading(self, *args, **kwargs):
        """Метод повинен викликатись перед початком читання даних"""
        global work 
        global r1, r2, r3, r4, r5
        global accname, coordname, dtname, pgpsname, pname
        r1 = reader(open(accname))
        r1.__next__()
        r2 = reader(open(coordname))
        r2.__next__()
        r3 = reader(open(dtname))
        r3.__next__()
        r4 = reader(open(pgpsname))
        r4.__next__()
        r5 = reader(open(pname))
        r5.__next__()
        work = True
    
    def stopReading(self, *args, **kwargs):
        """Метод повинен викликатись для закінчення читання даних"""
        global work 
        work = False
    