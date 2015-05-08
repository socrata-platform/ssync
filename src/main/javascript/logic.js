var ssyncleton = new DatasyncUpload({
    // log : function(thing) { console.log(thing); },
    forceWorkerReload : true
});

var controlFileEditor = undefined;

$(document).ready(function() {
    var ff = localStorage.getItem("fourfour");
    if(ff !== null) $("#fourfour").val(ff);

    controlFileEditor = ace.edit("control");
    var editorSession = controlFileEditor.getSession();
    editorSession.setMode("ace/mode/json");
    editorSession.setTabSize(2);
    editorSession.setUseSoftTabs(true);

    var cf = localStorage.getItem("control");
    if(cf !== null) {
        controlFileEditor.setValue(cf, -1);
    }
});

function go() {
    function msg(s) {
        $("#message").text(s);
    };

    function ds_log(s) {
        $("#log").text(s);
    };

    function alertJS(x) {
        alert(JSON.stringify(x));
    };

    var ff = $("#fourfour").val();
    var file = $("#input")[0].files[0];
    if(!file) {
        msg("No file specified");
        return;
    }
    var controlFileRaw = controlFileEditor.getValue();

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
    function onError(msg) {
        $("#go").prop("disabled", false);
        alertJS(msg);
    }
    function onComplete(x) {
        progress(x);
        $("#go").prop("disabled", false);
    };
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
    localStorage.setItem("fourfour", ff);
    localStorage.setItem("control", controlFileRaw);
    $("#go").prop("disabled", true);
    ssyncleton.upload(ff, controlFile, file,
                      { progress : progress,
                        onError : onError,
                        onComplete : onComplete,
                        headers : headers });
}
