import asyncio
import time
import numpy as np
import velodyne_decoder as vd
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from threading import Thread, Lock
from collections import deque
import socket

scan_buffer = deque(maxlen=3) #automatic memory management
data_lock = Lock()
running = False
has_new_data = False

config = vd.Config()
config.timestamp_first_packet = True
config.min_range = 5.0
config.max_range = 100.0
                        
def streamAll():
    global running
    global has_new_data
    print("Streaming from PCAP")
    try:
        for stamp, points in vd.read_pcap(pcap_file='./data.pcap', config=config):
            if not running:
                break                
            with data_lock:
                scan_buffer.append(points)
                total_points = sum(len(s) for s in scan_buffer)
                has_new_data = True
            time.sleep(0.1)
    except Exception as e:
        print(f"PCAP error: {e}")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.websocket("/ws")
async def endpoint(websocket: WebSocket):
    global has_new_data
    await websocket.accept()
    print("Client connected")
    try:
        while True:
            if has_new_data:
                print('reached newdata')
                with data_lock:
                    print('reached datalock')
                    array = np.concatenate(list(scan_buffer)) if len(scan_buffer) > 1 else scan_buffer[0]
                    has_new_data = False
                    points = points = np.ascontiguousarray(array[:, :3], dtype=np.float32)
                    
                    xyz = points.tobytes()
                    await websocket.send_bytes(xyz)
            await asyncio.sleep(0.1)
    except Exception as e:
        print(f"Error: {e}")
                
def start():
    global running
    running = True
    t = Thread(target=streamAll, daemon=True)    
    t.start()
    print("started streaming thread")
    
    
def main():
    start()
    uvicorn.run(app=app, host="0.0.0.0", port=8000)
    
    
if __name__ == "__main__":
    main()
    
    """
        def start_streaming(self):
        self.running = True
        if self.pcap_file:
            t = Thread(target=self._stream_pcap_thread, daemon=True)
        elif self.bag_file:
            t = Thread(target=self._read_bag_thread, daemon=True)
        else:
            t = Thread(target=self._stream_udp_thread, daemon=True)
        t.start()
        print("Started streaming thread")
    """
    