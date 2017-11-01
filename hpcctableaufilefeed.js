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
        var hpccConnection;
        var connProtocol = "http";
        var hpccServer = "192.168.56.101";
        var hpccPort = "8010"
        if (tableau.connectionData != "")
        {
            hpccConnection = JSON.parse(tableau.connectionData);
            hpccServer = hpccConnection.server;
            hpccPort = hpccConnection.port;
        }

        var basicauth = "";
        if (tableau.authType = tableau.authTypeEnum.basic && tableau.username.length > 0 && tableau.password.length > 0)
            basicauth = btoa(tableau.username.toString() + ":" + tableau.password.toString())

        $.ajax(
                {
                    url: connProtocol + "://" + hpccServer + ":" + hpccPort + "/WsDfu/DFUQuery.json?ver_=1.36",
                    headers: { "Authorization": "Basic " + basicauth.toString()}, //How do we conditionally include/exclude this entry?
                    dataType: 'json',
                    error: function(xhr, error)
                    {
                        tableau.log("GetSchema: Error: Could not fetch HPCC file list");
                        tableau.log(xhr.statusText);
                        schemaCallback([]);
                    },
                    success: function(json)
                    {
                        var hpccFileName = "";
                        var files = json.DFUQueryResponse.DFULogicalFiles.DFULogicalFile;

                        /*var cols = [];
                        cols.push(
                                {
                                    "id" : "col1",
                                    "description" : "firstcol",
                                    "dataType" : tableau.dataTypeEnum.string
                                });
*/
                        //tableschemas = [];
    //                    var tableschemas = {id:"hello", alias:"study", columns:cols};
                        //tableau.log("Resulting schema: " + JSON.stringify(tableSchemas));
                        var stringFiles = "[ ";
                        for (var fileindex = 0, len = files.length; fileindex < len; fileindex++)
                        //for (var fileindex = 1, len = 2; fileindex < len; fileindex++)
                        {
                            hpccFileName = files[fileindex].Name;

                            $.ajax(
                                    {
                                        url: connProtocol + "://" + hpccServer + ":" + hpccPort +  "/WsDfu/DFUGetFileMetaData.json?ver_=1.36&LogicalFileName=" + hpccFileName,
                                        headers: { "Authorization": "Basic " + basicauth.toString()}, //How do we conditionally include/exclude this entry?
                                        dataType: 'json',
                                        error: function(xhr, error)
                                        {
                                            tableau.log("GetSchema: Error: Could not fetch metadata for HPCC File: " + hpccFileName);
                                            tableau.log(xhr.statusText);
                                        },
                                        success: function(filecoldataresp)
                                        {
                                            var metacols = [];
                                            var dfucols = filecoldataresp.DFUGetFileMetaDataResponse.DataColumns.DFUDataColumn;
                                            for (var colindex = 0, len = dfucols.length; colindex < len; colindex++)
                                            {
                                                var dfucol = dfucols[colindex];
                                                if (dfucol.ColumnLabel != "__fileposition__")
                                                {
                                                    var fielddescription = "EclType: " + dfucol.ColumnEclType;
                                                    metacols.push(
                                                            {
                                                                "id" : dfucols[colindex].ColumnLabel,
                                                                "description" : fielddescription,
                                                                "dataType" : translateECLtoTableauTypes(dfucol.ColumnEclType)
                                                            });
                                                }
                                            }

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
                                                description: "Filenameadfasdf"
                                            };
                                            //tableau.log("TableSchema: " + JSON.stringify(tableSchema))
                                            //var tableCopy = JSON.parse(JSON.stringify(tableSchema));
                                            //schemaCallback([tableSchema]);
                                            //tableschemas.push(tableCopy);
                                            stringFiles = stringFiles + JSON.stringify(tableSchema);
                                        }
                                    });
                            if (fileindex + 1 < len)
                                stringFiles = stringFiles + ","
                        }
                        stringFiles = stringFiles + " ]";
                        tableau.log("JSON: " + stringFiles);
                        schemaCallback(JSON.parse(stringFiles));
                    }
                });
    };

    /*************************************************************
    * Get Data function
    *************************************************************/
    myConnector.getData = function(table, doneCallback)
    {
        var tablename = table.tableInfo.alias; // If reported by hpcc schema,
                                                // this contains actual hpcc name
        if (tablename === "")
            tablename = table.tableInfo.id; // Tableau filenames are very
                                            // restrictive
        var hpccConnection;

        var connProtocol = "http";
        var hpccServer = "192.168.56.101";
        var hpccPort = "8010"
        if (tableau.connectionData != "")
        {
            hpccConnection = JSON.parse(tableau.connectionData);
            hpccServer = hpccConnection.server;
            hpccPort = hpccConnection.port;
        }

        var basicauth = "";
        if (tableau.authType = tableau.authTypeEnum.basic && tableau.username.length > 0 && tableau.password.length > 0)
            basicauth = btoa(tableau.username.toString() + ":" + tableau.password.toString())

        /*var fileTotalCountURL = connProtocol + "://" + hpccServer + ":" + hpccPort + "/WsDfu/DFUBrowseData.json?LogicalName=" + tablename + "&SchemaOnly=1";
        $.ajax({
            url: fileTotalCountURL.toString(),
            headers: { "Authorization": "Basic " + basicauth.toString()}, //How do we conditionally include/exclude this entry?
            dataType: 'json',
            error: function(xhr, error)
            {
                tableau.log("GetData: Error: Could not fetch total record count for HPCC File: " + tablename);
                tableau.log(xhr.statusText);
                doneCallback();
            },
            success: function(totresp)
            {


                */
            var MAXRECS = 1000;

        var lastId = parseInt(table.incrementValue || -1);

        var fileDataFetchURL = connProtocol + "://" + hpccServer + ":" + hpccPort + "/WsDfu/DFUBrowseData.json?LogicalName=" + tablename + "&Start=0&Count=" + MAXRECS.toString();

        $.ajax({
                    url: fileDataFetchURL.toString(),
                    headers: { "Authorization": "Basic " + basicauth.toString()}, //How do we conditionally include/exclude this entry?
                    dataType: 'json',
                    error: function(xhr, error)
                    {
                        tableau.log("GetData: Error: Could not fetch data for HPCC File: " + tablename);
                        tableau.log(xhr.statusText);
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
                        }
                        else
                        {
                            var filetotal = resp.DFUBrowseDataResponse.Total;
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
                            table.appendRows(tableData);
                        }

                        doneCallback();
                    }
        });
            //}
        //});
    };

    tableau.registerConnector(myConnector);

    $(document).ready(function()
    {
        $("#submitButton").click(function(){
            if ($('#hpcc-password').val().trim().length == 0)
            {
                tableau.abortForAuth();
            }
            else
                {
            var connObj =
            {
                    protocol: $('#conn-protocol').val().trim(),
                    server: $('#hpcc-server').val().trim(),
                    port: $('#hpcc-port').val().trim(),
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
         var hpccConnection;

         var connProtocol = "http";
         var hpccServer = "192.168.56.101";
         var hpccPort = "8010"

        if (tableau.connectionData != "")
        {
            hpccConnection = JSON.parse(tableau.connectionData);
            hpccServer = hpccConnection.server;
            hpccPort = hpccConnection.port;
        }

        var fileDataFetchURL = connProtocol + "://" + hpccServer + ":" + hpccPort;

        $.ajax(
                {
                    url: fileDataFetchURL.toString(),
                    dataType: 'json',
                    error: function(xhr, error)
                    {
                        tableau.authType = tableau.authTypeEnum.basic;
                    },
                    success: function(resp)
                    {
                        tableau.authType = tableau.authTypeEnum.none;
                    }
                }
            );

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

/*
function fetchFileInfo(url, filename, cb)
{
    var obj = new XMLHttpRequest();
    obj.overrideMimeType("application/json");
    obj.open("GET", url, true);

    obj.onreadystatechange = function() {
        if (obj.readyState == 4 && obj.status == "200") {
            cb(obj.responseText, filename);
        }
    }
    obj.send(null);
}

function loadJSON(url, cb) {
    var obj = new XMLHttpRequest();
    obj.overrideMimeType("application/json");
    obj.open("GET", url, true);

    obj.onreadystatechange = function() {
        if (obj.readyState == 4 && obj.status == "200") {
            cb(obj.responseText);
        }
    }
    obj.send(null);
}
*/
