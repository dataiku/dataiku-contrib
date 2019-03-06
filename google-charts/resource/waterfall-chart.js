let dataReady;
let chartReady;

function initSankey(onReady) {
    try {
        dataiku.checkWebAppParameters();
    } catch (e) {
        webappMessages.displayFatalError(e.message + ' Go to settings tab.');
        return;
    }
    
    const cfg = dataiku.getWebAppConfig();
    const dataset = cfg['dataset'];
    const unit = cfg['unit'];
    const value = cfg['value'];
    const sampling = {};

    let allRows;
    let old_record = null;
    
    function transform_record(record){
        var row = [];
        row.push(record[unit]);
        if (old_record == null){
            row.push("0");
            row.push("0");
        }
        else {
            row.push(old_record[value]);
            row.push(old_record[value]);
        }

        row.push(record[value]);
        row.push(record[value]);

        old_record = record;
        return row;
    }

    function drawAppIfEverythingReady() {
        if (!chartReady || !dataReady) {
            return;
        }
        onReady(allRows);
    }

    if (!window.google) {
        webappMessages.displayFatalError('Failed to load Google Charts library. Check your connection.');
    } else {
        google.charts.load('current', {'packages':['corechart']});
        google.charts.setOnLoadCallback(function() {
            chartReady = true;
            drawAppIfEverythingReady();
        });
        dataiku.fetch(dataset, sampling, function(dataFrame) {

            allRows = dataFrame.mapRecords(transform_record);
            //allRows = dataFrame.mapRecords(r => [r[unit], r[value]]); 
            console.log(allRows);
            dataReady = true;
            drawAppIfEverythingReady();
        });
    }
}