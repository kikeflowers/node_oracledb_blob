(function() {
    var $filesToUpload,
        $fileInput,
        $fileUpload,
        $filesInDb,
        fileIdMap;

    $(document).ready(init);

    function init() {
        $('#file-input-wrapper').html('<input id="file-input" type="file" multiple>');

        $filesToUpload = $('#files-to-upload').find('tbody');
        $fileInput = $('#file-input');
        $fileUpload = $('#file-upload');

        $fileUpload.on('click', handleUploadClick);

        $filesInDb = $('#files-in-db').find('tbody');

        $filesInDb.on('click', '.delete', deleteFile);

        if (window.File && window.FileReader && window.FileList && window.Blob) {
            $fileInput.on('change', handleFileSelect);
        } else {
            $('#browser-support-warning').show();
        }

        refreshFilesInDatabase();
    }

    function handleFileSelect(event) {
        var idx,
            newFilesHtml = '',
            selectedFiles;

        fileIdMap = {};

        $filesToUpload.empty();

        selectedFiles = event.target.files;

        if (selectedFiles.length) {
            for (idx = 0; idx < selectedFiles.length; idx += 1) {
                fileIdMap[selectedFiles[idx].name] = idx;

                newFilesHtml +=
                    '<tr id="file-id-' + fileIdMap[selectedFiles[idx].name] + '">' +
                    '<td>' + selectedFiles[idx].name + '</td>' +
                    '<td>' + selectedFiles[idx].type + '</td>' +
                    '<td>' + selectedFiles[idx].size + '</td>' +
                    '<td>' + selectedFiles[idx].lastModifiedDate + '</td>' +
                    '<td>' +
                        '<div class="progress">\n' +
                            '<div class="progress-bar progress-bar-striped active" role="progressbar" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100" style="width: 0%">\n' +
                                '<span class="sr-only">0%</span>\n' +
                            '</div>\n' +
                        '</div>' +
                    '</td></tr>\n';
            }

            $filesToUpload.html(newFilesHtml);

            $fileUpload.prop('disabled', false);
        } else {
            $fileUpload.prop('disabled', true);
        }
    }

    function handleUploadClick() {
        var selectedFiles,
            maxSyncUploads = 3;

        $fileUpload.prop('disabled', true);

        selectedFiles = $fileInput[0].files;

        async.eachLimit(
            selectedFiles,
            maxSyncUploads,
            uploadFile,
            reset
        );
    }

    function uploadFile(file, callback) {
        var xhr = new XMLHttpRequest();
        var $row = $filesToUpload.find('#file-id-' + fileIdMap[file.name]);
        var $progressBar = $row.find('.progress-bar');

        xhr.open('POST', '/api/files', true);

        xhr.setRequestHeader('x-file-name', file.name);
        xhr.setRequestHeader('x-content-type', file.type);

        xhr.onload = function(e) {
            if (e.currentTarget.status === 200) {
                $row.remove();
                refreshFilesInDatabase();
            } else if (e.currentTarget.status >= 400 && e.currentTarget.status <= 505) {
                var errorHtml = '<span class="label label-warning" data-placement="top" title="' +
                    e.currentTarget.responseText +
                    '"><span class="glyphicon glyphicon-exclamation-sign" aria-hidden="true"></span> Error</span>';

                $row.find('.progress').closest('td')
                    .empty()
                    .html(errorHtml)
                        .find('.label')
                        .tooltip();
            }

            callback();
        };

        xhr.upload.onprogress = function(e) {
            var percentComplete;

            if (e.lengthComputable) {
                percentComplete = (e.loaded / e.total) * 100;

                $progressBar
                    .attr('aria-valuenow', percentComplete)
                    .css('width', percentComplete + '%')
                    .html('<span class="sr-only">' + percentComplete + '%</span>');
            }
        };

        xhr.send(file);
    }

    function reset(err) {
        //need to replace the input as there's no other way to clear the files selected
        $('#file-input-wrapper').html('<input id="file-input" type="file" multiple>');

        $fileInput = $('#file-input');

        $fileInput.on('change', handleFileSelect);
    }

    
   
    function refreshFilesInDatabase() {

        

        

        $.ajax({
            url:'/api/files',
            type:'GET',
            dataType: 'json',
            contentType: "application/json;",
            success:function(data){
                var newRowsHtml = '';

            data.rows.forEach(function(file) {

                newRowsHtml +=
                    '<tr>' +
                    '<td class="file-name">' + file.FILE_NAME + '</td>' +
                    '<td>' + file.FILE_TYPE + '</td>' +
                    '<td>' + file.FILE_LENGTH + '</td>' +
                    // '<td><a class="btn btn-primary btn-xs" href="/api/file/' + encodeURIComponent(file.FILE_NAME) + '" role="button">' +
                    // '    <span class="glyphicon glyphicon-download" aria-hidden="true"></span> Download</a>' +
                    // '</td>' +
                     '<td><img src="/api/filess/'+file.FILE_NAME+'"></td>' + 
                    '<td>' +
                    '    <button id="file-upload" class="btn btn-danger delete btn-xs" type="button">' +
                    '        <span class="glyphicon glyphicon-trash" aria-hidden="true"></span> Delete' +
                    '    </button>' +
                    '</td>' +
                    '</tr>\n';
            });

            $filesInDb.html(newRowsHtml);

            },
            error:function(res){
                alert("Bad thing happend! " + res.statusText);
            }
        });
        
        
    }

    function deleteFile() {
        var $tr,
            fileName;

        $tr = $(this).closest('tr');
        fileName = $tr.find('.file-name').text();

        $.ajax({
            url: '/api/file/' + fileName,
            method: 'delete'
        }).done(function() {
            $tr.remove();
        });
    }
}());
