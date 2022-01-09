var materialChart;
var MSG_RESET_THRESHOLD = 10;

$(document).ready(function() {
    google.charts.load('current', {packages: ['corechart', 'bar']});
    google.charts.setOnLoadCallback(drawTitleSubtitle);

    setup_websocket_consumer();
});

function setup_websocket_consumer(){
    var hashtag_counts = {};
    var last_message_count = 0;  // update every 5 messages

    let socket = new WebSocket(getWebsocketUrl());

    socket.onopen = function(e) {
        console.log("[open] Connection established");
        console.log("Sending to server");
        socket.send("Just a hello");
    };

    socket.onmessage = function(event) {
//        console.log(`[message] Data received from server: ${event.data}`);
        var data = JSON.parse(event.data);
        if(data.hasOwnProperty('key')){
            hashtag_counts[data.key] = parseInt(data.value);
            last_message_count += 1;
            if (last_message_count >= MSG_RESET_THRESHOLD) {
                last_message_count = 0;
                updateChart(hashtag_counts);
            }
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

function formatChartData(hashtag_counts){
    // Sort by count
    toDict = Object.entries(hashtag_counts);

    // output as [[hdr1, hdr2], [key, val], [key, val]...]
    toDict.sort(function (a, b) {
        return b[1] - a[1];
    });

    toDict.unshift(['Hashtag', 'Count']);
    return toDict;
}

function updateChart(hashtag_counts){
    var data = formatChartData(hashtag_counts);
    var ccdata = google.visualization.arrayToDataTable(data);
    materialChart.draw(ccdata, materialChart.materialOptions);
    console.log(JSON.stringify(data));
}


function drawTitleSubtitle() {
    var data = google.visualization.arrayToDataTable([
        ['Hashtag', 'Count'],
    ]);

    materialChart = new google.charts.Bar(document.getElementById('chart_div'));

    materialChart.materialOptions = {
        chart: {
            title: 'Hashtag counts',
        },
        hAxis: {
            title: 'Count',
            minValue: 0,
        },
        vAxis: {
            title: 'Hashtag'
        },
        bars: 'horizontal'
    };

    materialChart.draw(data, google.charts.Bar.convertOptions(materialChart.materialOptions));
}