/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload and metadata.
 * @param {!Function} callback Callback function to signal completion.
 */
var BigQuery = require('@google-cloud/bigquery');
var projectId = 'proj-saqher20-1'; // IMPORTANT!! Enter your project ID here
var datasetName = 'iotData';
var tableName = 'event_data'; // Should be 'event_data'    -- Grp11

var bigquery = new BigQuery({
    projectId: projectId,
});

exports.subscribe = function (event, callback) {
    var msg = event.data;
    var incomingData = msg
        ? Buffer.from(msg, 'base64').toString()
        : '{"timestamp":"1970-01-01 00:00:00","device_id":"", "moved": "0", "accel_x":"0", "accel_y":"0", "accel_z":"0", "direction": "0" }';
    var data = JSON.parse(incomingData);

    bigquery
        .dataset(datasetName)
        .table(tableName)
        .insert(data)
        .then(function () {
            console.log('Inserted rows');
            callback(); // task done 
        })
        .catch(function (err) {
            if (err && err.name === 'PartialFailureError') {
                if (err.errors && err.errors.length > 0) {
                    console.log('Insert errors:');
                    err.errors.forEach(function (err) {
                        console.error(err);
                    });
                }
            } else {
                console.error('ERROR:', err);
            }

            callback(); // task done 
        });
};
