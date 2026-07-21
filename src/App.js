export default function Hello () {
  const websocket = new WebSocket('ws://localhost:8000/ws');

  websocket.binaryType = 'arraybuffer';
  
  websocket.addEventListener("open", () => {
    console.log("hello");
  });
  
  websocket.addEventListener("message", (m) => {
    console.log(m.data);
  });
}