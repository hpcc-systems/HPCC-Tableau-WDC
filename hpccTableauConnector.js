/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */


$.ajaxSetup({
   async: false
 });

(function()
{
    var myConnector = tableau.makeConnector();

    /*************************************************************
    * Get Schema function
    *************************************************************/
    myConnector.getSchema = function(schemaCallback)
    {
        var eclwatchurl = "http://localhost:8010";
        var basictoken = "";
        var fileprefix = "";
        var filename = "";
        var fieldid = "";

        if (tableau.connectionData != "")
        {
            var hpccConnection = JSON.parse(tableau.connectionData);
            eclwatchurl = hpccConnection.url;
            basictoken = hpccConnection.basic;
            fileprefix = hpccConnection.fileprefix
            filename = hpccConnection.file;
            fieldid = hpccConnection.field;
        }
        var dfuqueryurl = eclwatchurl +  "/WsDfu/DFUQuery.json?ver_=1.36";
        if (filename.length != 0)
            dfuqueryurl = dfuqueryurl + "&LogicalName=" + filename;
        else if (fileprefix != 0)
            dfuqueryurl = dfuqueryurl + "&Prefix=" + fileprefix;
        var stringFiles = "[ ";
        $.ajax(
                {
                    url: dfuqueryurl.toString(),
                    headers: { "Authorization": "Basic " + basictoken.toString()}, //How do we conditionally include/exclude this entry?
                    dataType: 'json',
                    error: function(xhr, error)
                    {
                        tableau.log("GetSchema: Error: Could not fetch HPCC file list");
                        tableau.log(xhr.statusText);
                        schemaCallback([]);
                    },
                    success: function(filessearchresp)
                    {

                          if ('exceptions' in filessearchresp)
                          {
                              for (var ei = 0, len = filessearchresp.exceptions.exception.length; ei < len; ei++)
                              {
                                  tableau.log(filessearchresp.exceptions.exception[ei].message);
                              }
                          }
                          else
                          {
                            var hpccFileName = "";
                            var files = filessearchresp.DFUQueryResponse.DFULogicalFiles.DFULogicalFile;

                            var filemetafetched = false;
                            for (var fileindex = 0, len = files.length; fileindex < len; fileindex++)
                            {
                                hpccFileName = files[fileindex].Name;
                                $.ajax(
                                        {
                                            url: eclwatchurl +  "/WsDfu/DFUGetFileMetaData.json?ver_=1.36&LogicalFileName=" + hpccFileName +" &basic=" + basictoken,
                                            headers: { "Authorization": "Basic " + basictoken.toString()}, //How do we conditionally include/exclude this entry?
                                            dataType: 'json',
                                            timeout: 5000,
                                            //async: true,
                                            error: function(xhr, error)
                                            {
                                                tableau.log("GetSchema: Error: Could not fetch metadata for HPCC File: " + hpccFileName);
                                                tableau.log(xhr.statusText);
                                            },
                                            success: function(filecoldataresp)
                                            {
                                                var metacols = [];
                                                if ('Exceptions' in filecoldataresp)
                                                {
                                                    for (var ei = 0, len = filecoldataresp.Exceptions.Exception.length; ei < len; ei++)
                                                    {
                                                        tableau.log(filecoldataresp.Exceptions.Exception[ei].Message);
                                                    }
                                                }
                                                else
                                                {
                                                    var dfucols = filecoldataresp.DFUGetFileMetaDataResponse.DataColumns.DFUDataColumn;
                                                    var inccolfound = false;
                                                    for (var colindex = 0, len = dfucols.length; colindex < len; colindex++)
                                                    {
                                                        var dfucol = dfucols[colindex];
                                                        if (fieldid == dfucol.ColumnLabel)
                                                            inccolfound = true;

                                                        if (dfucol.ColumnLabel != "__fileposition__")
                                                        {
                                                            var fielddescription = "EclType: " + dfucol.ColumnEclType;
                                                            metacols.push(
                                                                    {
                                                                        "id" : dfucol.ColumnLabel,
                                                                        "description" : fielddescription,
                                                                        "dataType" : translateECLtoTableauTypes(dfucol.ColumnEclType)
                                                                    });
                                                        }
                                                    }
                                                    if (hpccFileName != filename)
                                                        inccolfound = false;
                                                    // HPCC file scope delimiters ":", and ".", "-" are not legal tableau file name chars
                                                    // there might be other chars that need filtering
                                                    var normalizedfilename = hpccFileName.split(':').join('_');
                                                    normalizedfilename = normalizedfilename.split('.').join('_');
                                                    normalizedfilename = normalizedfilename.split('-').join('_');

                                                    var tableSchema =
                                                    {
                                                        id : normalizedfilename.toString(),
                                                        alias : hpccFileName.toString(),
                                                        columns : metacols,
                                                        description: "",
                                                        incrementColumnId: inccolfound ? fieldid.toString() : ""
                                                    };
                                                    if (!filemetafetched)
                                                         filemetafetched = true;
                                                    else
                                                        stringFiles = stringFiles + " , ";
                                                    stringFiles = stringFiles + JSON.stringify(tableSchema);
                                                }
                                            }
                                        });
                            }
                        }
                    }
                });
        stringFiles = stringFiles + " ]";
        console.log("JSON: " + stringFiles);
        schemaCallback(JSON.parse(stringFiles))
    };

    /*************************************************************
    * Get Data function
    *************************************************************/
    myConnector.getData = function(table, doneCallback)
    {
        var MAXRECS = 1000;

        var tablename = table.tableInfo.alias; // If reported by hpcc schema,
                                                // this contains actual hpcc name
        if (tablename === "")
            tablename = table.tableInfo.id; // Tableau filenames are very
                                            // restrictive

        var eclwatchurl = "http://localhost:8010";
        var basictoken = "";

        if (tableau.connectionData != "")
        {
            var hpccConnection = JSON.parse(tableau.connectionData);
            eclwatchurl = hpccConnection.url;
            basictoken = hpccConnection.basic;
        }

        var lastId = parseInt(table.incrementValue || 0);
        var filetotal = 0;
        var rowsread = 0;
        var stop = false;
        while (rowsread <= filetotal && !stop)
        {

        $.ajax({
                    url: eclwatchurl + "/WsDfu/DFUBrowseData.json?LogicalName=" + tablename + "&Start="+ rowsread + "&Count=" + MAXRECS.toString(),
                    headers: { "Authorization": "Basic " + basictoken.toString()}, //How do we conditionally include/exclude this entry?
                    dataType: 'json',
                    error: function(xhr, error)
                    {
                        tableau.log("GetData: Error: Could not fetch data for HPCC File: " + tablename);
                        tableau.log(xhr.statusText);
                        stop = true;
                        doneCallback();
                    },
                    success: function(resp)
                    {
                        if ('Exceptions' in resp)
                        {
                            for (var ei = 0, len = resp.Exceptions.Exception.length; ei < len; ei++)
                            {
                                if (resp.Exceptions.Exception[ei].Code === 20063)
                                {
                                    tableau.log("GetData ("+tablename+"): Error: Limited to " + MAXRECS + " records");
                                }
                                else
                                {
                                    tableau.log(resp.Exceptions.Exception[ei].Message);
                                }
                            }
                            stop = true;
                            doneCallback();
                        }
                        else
                        {
                            filetotal = resp.DFUBrowseDataResponse.Total;
                            if (filetotal > MAXRECS)
                            {
                                tableau.log("GetData: WARNING: data fetches are limited to 10000 records out of " + filetotal + " total records." );
                            }
                            var result = resp.DFUBrowseDataResponse.Result;
                            var tableData = [];

                            //result has multiple roots ie, not valid XML, we need to "root" it.
                            var rootedresult = "<root>" + result + "</root>";
                            var parser = new DOMParser();                                     //is this expensive?
                                                                                            //should we do this once and reuse? is it thread safe?
                            var xmlDoc = parser.parseFromString(rootedresult,"text/xml");     //important to use "text/xml"
                            var rows = xmlDoc.getElementsByTagName("Row");
                            if (rows.length === 0)
                            {
                                stop = true;
                                doneCallback();
                            }

                            rowsread += rows.length;
                            var tableData = [];
                            for (var rowindex = 0, len = rows.length; rowindex < len; rowindex++)
                            {
                                var row = rows[rowindex];
                                var fields = row.childNodes;
                                var jsonRow = {};

                                for (var fieldindex = 0, fieldcount = fields.length; fieldindex < fieldcount; fieldindex++)
                                {
                                    jsonRow[fields[fieldindex].tagName] = fields[fieldindex].textContent;
                                }
                                tableData.push(jsonRow);
                            }
                            table.incrementValue = lastId + rows.length;
                            table.appendRows(tableData);
                        }

                        //doneCallback();
                    }
        });
        doneCallback();
        }
    };

    tableau.registerConnector(myConnector);

    $(document).ready(function()
    {
        $("#submitButton").click(function()
          {
            if ($('#hpcc-single-file').is(":checked") == true && $('#hpcc-file-prefix').val().trim().length == 0)
            {
                $('#errorMsg').show().html('Please enter target HPCC file name, or change fetch criteria');
            }
            else if ($('#hpcc-file-filter').is(":checked") == true && $('#hpcc-file-prefix').val().trim().length == 0)
            {
                $('#errorMsg').show().html('Please enter an HPCC file name filter, or change fetch criteria');
            }
            else if ($('#hpcc-fetchall').is(":checked") == false && $('#hpcc-file-prefix').val().trim().length == 0)
            {
                $('#errorMsg').show().html('Please enter File Fetch Criteria. ("All Files" - not recommended)');
            }
            else if ($('#hpcc-password').val().trim().length == 0)
            {
                $('#errorMsg').show().html('Please enter Password. <br> (Tableau Desktop requirement even if not needed by your HPCC)');
            }
            else
            {
                var inputProtocol = $('#conn-protocol').val().trim();
                if (inputProtocol == "")
                    inputProtocol = "http";

                var inputServer = $('#hpcc-server').val().trim();
                   if (inputServer == "")
                       inputServer = "localhost";

                   var inputPort = $('#hpcc-port').val().trim();
                if (inputPort == "")
                    inputPort = "8010";

                var hpccurl = inputProtocol + "://" + inputServer + ":" + inputPort;

                var inputUser = $('#hpcc-user').val().trim();
                var inputPass = $('#hpcc-password').val().trim();
                var basicauth = "";
                if (inputUser != "" && inputPass != "")
                    basicauth = btoa(inputUser + ":" + inputPass)

                var connObj =
                {
                    url: hpccurl,
                    basic: basicauth,
                    field:  $('#hpcc-fieldid').val().trim(),
                    file:  $('#hpcc-single-file').is(":checked") == true ? $('#hpcc-file-prefix').val().trim() : '',
                    fileprefix: $('#hpcc-file-filter').is(":checked") == true ? $('#hpcc-file-prefix').val().trim() : '',
                };

                tableau.username = $('#hpcc-user').val().trim();
                tableau.password = $('#hpcc-password').val().trim()

                tableau.connectionData = JSON.stringify(connObj);
                tableau.connectionName = "HPCC Data Connection";
                tableau.submit();
            }
        });
    });

     myConnector.init = function(initCallback)
     {
          tableau.authType = tableau.authTypeEnum.basic;
          initCallback();
      };

})();

/*************************************************************
* Maps ECL types to Tableau types
*************************************************************/
function translateECLtoTableauTypes(eclType)
{
    eclType == eclType.toLowerCase();
    if (typeof(eclType) != "undefined")
        return tableau.dataTypeEnum.string;
    else if (eclType == "")
        return tableau.dataTypeEnum.string;
    else if (eclType.startsWith("string"))
        return tableau.dataTypeEnum.string;
    else if (eclType.startsWith("unsigned"))
        return tableau.dataTypeEnum.int;
    else if (eclType === "boolean")
        return tableau.dataTypeEnum.bool;
    else if (eclType.startsWith("qstring"))
        return tableau.dataTypeEnum.string;
    else if (eclType.startsWith("float"))
        return tableau.dataTypeEnum.float;
    else if (eclType.startsWith("int"))
        return tableau.dataTypeEnum.int;
    else if (eclType.startsWith("decimal"))
        return tableau.dataTypeEnum.float;
    else if (eclType.startsWith("short"))
        return tableau.dataTypeEnum.string;
    else if (eclType.startsWith("datetime"))
        return tableau.dataTypeEnum.datetime;
    else if (eclType.startsWith("date"))
        return tableau.dataTypeEnum.date;
    else if (eclType.startsWith("time"))
        return tableau.dataTypeEnum.datetime;
    else if (eclType.startsWith("real"))
        return tableau.dataTypeEnum.float;
    else if (eclType.startsWith("unicode"))
        return tableau.dataTypeEnum.string;
    else if (eclType.startsWith("duration"))
        return tableau.dataTypeEnum.string
        //mapECLTypeNameToSQLType.put("DOUBLE", java.sql.Types.DOUBLE);
        //mapECLTypeNameToSQLType.put("LONG", java.sql.Types.NUMERIC);
        //mapECLTypeNameToSQLType.put("GDAY", java.sql.Types.DATE);
        //mapECLTypeNameToSQLType.put("GMONTH", java.sql.Types.DATE);
        //mapECLTypeNameToSQLType.put("GYEAR", java.sql.Types.DATE);
        //mapECLTypeNameToSQLType.put("GYEARMONTH", java.sql.Types.DATE);
    else
        return tableau.dataTypeEnum.string;
}

function changeLabel(labelId, newLabelValue) {
    var targetLabel = document.getElementById(labelId);
    if (targetLabel)
        targetLabel.innerHTML  = newLabelValue;
}

function toggle(checkboxId, toggleId) {
    var checkbox = document.getElementById(checkboxId);
    var fieldToToggle = document.getElementById(toggleId);
    fieldToToggle.disabled = checkbox.checked;
}
