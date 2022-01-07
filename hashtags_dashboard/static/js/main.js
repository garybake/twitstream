$(document).ready(function() {
    setup_websocket_consumer();
});

function setup_websocket_consumer(){
    var hashtags = {};

    let socket = new WebSocket(getWebsocketUrl());

    socket.onopen = function(e) {
      console.log("[open] Connection established");
      console.log("Sending to server");
      socket.send("Just a hello");
    };

    socket.onmessage = function(event) {
      console.log(`[message] Data received from server: ${event.data}`);
      var data = JSON.parse(event.data);
      if(data.hasOwnProperty('key')){
        hashtags[data.key] = parseInt(data.value);
      }
    };

    socket.onclose = function(event) {
      if (event.wasClean) {
        console.log(`[close] Connection closed cleanly, code=${event.code} reason=${event.reason}`);
      } else {
        console.log('[close] Connection died');
      }
    };

    socket.onerror = function(error) {
      console.log(`[error] ${error.message}`);
    };
}

function getWebsocketUrl(){
    const url = new URL(window.location.href);
    return `ws://${url.hostname}:${url.port}/consumer`
}