import asyncio
import argparse
import time
import numpy as np
import velodyne_decoder as vd
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from threading import Thread, Lock
from collections import deque
import json
import socket

scan_buffer = deque(maxlen=3) #automatic memory management
data_lock = Lock()
running = False
has_new_data = False
scan_count = 0

config = vd.Config()
config.timestamp_first_packet = True
config.min_range = 5.0
config.max_range = 100.0
                        
def streamAll(port, data):
    if data:        
        print(f"Streaming from PCAP: {data}")
        try:
            for stamp, points in vd.read_pcap(pcap_file=data, config=config):
                if not running:
                    break
                scan_count += 1
                with data_lock:
                    scan_buffer.append(points)
                    total_points = sum(len(s) for s in scan_buffer)
                    has_new_data = True
                # time.sleep(0.1)
            print(f"\nPCAP complete: {scan_count} scans")
        except Exception as e:
            print(f"PCAP error: {e}")
    elif port:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 26214400) #25mb
        
        try:
            sock.bind(('', port))
            sock.settimeout(0.1)
            print(f"Socket bound on UDP port {port}")
            
            stream_decoder = vd.StreamDecoder(config)
            print("velodyne_decoder StreamDecoder initialized")
            
            packet_count = 0
            
            try:
                data, addr = sock.recvfrom(2000)
                packet_count += 1
                
                current_time = time.time()
                result = stream_decoder.decode(current_time, data, False)
                
                if result is not None:
                    points = result.second if hasattr(result, 'second') else result[1]
                    if points is not None and len(points) > 0:
                        scan_count += 1
                        with data_lock:
                            scan_buffer.append(points)
                            total_points = sum(len(s) for s in scan_buffer)
                            has_new_data = True
                    if packet_count % 200 == 0:
                        print(f"\r  Packets: {packet_count} | Scans: {scan_count}", 
                              end='', flush=True)

            except Exception as e:
                if "TimePair" not in str(e):
                    print(f"\nUDP error: {e}")
        except Exception as e:
            print(f"Socket error: {e}")
        finally:
            sock.close()
            print("\nSocket closed")


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
    await websocket.send_text('world')
    await asyncio.sleep(5)
    
def main():
    uvicorn.run(app=app, host="0.0.0.0", port=8000)
    # streamAll(None, data='./data.pcap')
    
if __name__ == "__main__":
    main()