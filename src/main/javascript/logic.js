var ssyncleton = new DatasyncUpload({
    // log : function(thing) { console.log(thing); },
    forceWorkerReload : true
});

function go2(controlFileRaw) {
    function msg(s) {
        $("#message").text(s);
    };

    function ds_status(s) {
        $("#status").text(s);
    };

    function ds_log(s) {
        $("#log").text(s); // append(document.createTextNode(s));
    };
    function alertJS(x) {
        alert(JSON.stringify(x));
    };
    var file = $("#input")[0].files[0];
    var datasetId = $("#fourfour").val();
    function progress(prog) {
        if(prog.stage == ssyncleton.UploadingData) msg("read " + prog.file_bytes_read + " bytes ("+ (100*prog.file_bytes_read/prog.file_bytes_total).toFixed(3) + "%); sent " + prog.bytes_uploaded + " bytes");
        else if(prog.stage == ssyncleton.WatchingProgress) {
            if(prog.status !== undefined) msg(prog.status.english);
        } else msg(prog.stage);
        if(prog.log === undefined) ds_log("");
        else {
            var log = [];
            prog.log.forEach(function(le) {
                log.push(le.english)
            });
            ds_log(log.join("\n"));
        }
    };
    var onError = alertJS;
    function onComplete(x) { progress(x); alert("finished!\n" + JSON.stringify(x)) };
    headers = {
        "x-csrf-token" : encodeURIComponent($.cookie('socrata-csrf-token')),
        "x-app-token" : $("#apptoken").val()
    }
    var controlFile = undefined;
    try {
        controlFile = JSON.parse(controlFileRaw);
    } catch (e) {
        if(e instanceof SyntaxError) {
            alert("Invalid control file: " + e.message);
            return;
        }
        throw e;
    }
    localStorage.setItem("fourfour", datasetId);
    localStorage.setItem("control", controlFileRaw);
    ssyncleton.upload(datasetId, controlFile, file,
                      { progress : progress,
                        onError : onError,
                        onComplete : onComplete,
                        headers : headers });
}
