let allRows;
let webAppConfig = dataiku.getWebAppConfig()['webAppConfig'];

function draw() {

    console.warn('DRAWING ', allRows);
    let data = new google.visualization.DataTable();
    data.addColumn('string', 'label');
    data.addColumn('number', 'min_threshold');
    data.addColumn('number', 'min_value');
    data.addColumn('number', 'max_value');
    data.addColumn('number', 'max_threshold');
    
    for (var i = 0; i < allRows.length; i++) {
        arr = allRows[i];
        data.addRow([arr[0], parseInt(arr[1]), parseInt(arr[2]), parseInt(arr[3]), parseInt(arr[4])]);
    }

    let options = {
      legend: 'none',
      bar: { groupWidth: '100%' }, // Remove space between bars.
      candlestick: {
        fallingColor: { strokeWidth: 0, fill: '#a52714' }, // red
        risingColor: { strokeWidth: 0, fill: '#0f9d58' }   // green
      }
    };

    let chart = new google.visualization.CandlestickChart(document.getElementById('waterfall-chart'));
    chart.draw(data, options);
}
    
initWaterfall( webAppConfig, (data) => {
    allRows = data;
    draw(); 
});

window.addEventListener('message', function(event) {
    if (event.data) {
        webAppConfig = JSON.parse(event.data)['webAppConfig'];
        if (!allRows) {
            return;
        }
        initWaterfall(webAppConfig, (data) => {
            allRows = data;
            draw();
        });;
    }
});
