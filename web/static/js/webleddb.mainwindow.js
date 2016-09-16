(function($) {

    $.widget("webleddb.mainwindow", {
		options: {
			init:null,
			setUI:null,
            //callbacks
            _setBusy: null,
            getQueryObject: null,
            issueQuery: null
		},
				
		_create: function() {
            var offset =  this.element.offset();
            this.page_rows = 2000; // number of rows to show in single page. also used for step in sliders.
            this.element.
                addClass("ui-widget-content").
                css({
                    "width": $(window).width()-3*offset.left,
                    "height": $(window).height()-2*offset.top
                });
            $( "#messagebox" ).dialog({ autoOpen: false, modal: true });
        },
        
        setUI: function(){
            var _qoinfo = this._qmData.QUERY_OPTIONS_INFO;
            var _vars = this._qmData.QUERY_OPTIONS; //[ "lds", "ldsb", "ldsbp" ]; //each prefix is for one tab.
            var qso = this._qmData.QUERY_SELECTION_OPTIONS;
            var _qohead = this._qmData.QUERY_OPTIONS_HEADERS;
            //console.log( "Headers: ", _qohead );
            var mw = this.element;
            //create an array to track the fw updates.
            mw.fwupdated = new Array();
            for(var i = 0; i < _vars.length; i++){
                mw.fwupdated[i] = true;
            }
            var fw = $( "<div id=\"filterwidget\" class=\"ui-widget-container\"> </div>" );
            var fwcontent = $( "<div></div>" );
            fwcontent.filterwindow();
            fwcontent.filterwindow( "option", "init", {"qso":qso, "qo":_vars} );
            fw.append( fwcontent ).
               dialog({
                autoOpen:false,
                width:"330",
                height:"500",
                resizable: false,
                show:"slide",
                hide:"slide",
                modal: true,
                buttons: {
                    Ok : function(){
                        //make a JSON call to load the current view.
                        var curIndex =$( "#accordion" ).tabs('option', 'selected'); 
                        //reset the sliders
                        $( "#scroll_" + _vars[curIndex] ).slider( "option", "value", 0 );
                        var queryObject = mw.mainwindow( "getQueryObject", curIndex );
                        //set the update bit to true on all the previous tabs.
                        for(var i = curIndex; i > -1; i--){
                            mw.fwupdated[i] = true;
                        }
/*                        //remove anyselections in the previous tabs so that when we switch to those tabs a query is executed.
                        for(var i = curIndex; i > -1; i--){
                            $( "#datatable_" + _vars[i] ).datatable( "option", "selectall", false );
                        } */
                        fw.dialog( "close" );
                        mw.mainwindow( "issueQuery", {qo:queryObject,cb:null} );
                  },
                    Cancel : function(){
                        fw.dialog( "close" );
                    },
               }
            });
            
            var hw = $( "<div id=\"headerwidget\" class=\"ui-widget-container\"> </div>" );
            var hwcontent = $( "<div></div>" );
            hwcontent.headerwindow();
            hwcontent.headerwindow( "option", "init", {"qohead":_qohead, } );
            hw.append( hwcontent ).
               dialog({
                autoOpen:false,
                width:"330",
                height:"500",
                resizable: false,
                show:"slide",
                hide:"slide",
                modal: true,
                buttons: {
                    Ok : function(){
                        //make a JSON call to load the current view.
                        var curIndex =$( "#accordion" ).tabs('option', 'selected'); 
                        //reset the sliders
                        $( "#scroll_" + _vars[curIndex] ).slider( "option", "value", 0 );
                        var queryObject = mw.mainwindow( "getQueryObject", curIndex );
                        //set the update bit to true on all the previous tabs.
                        for(var i = curIndex; i > -1; i--){
                            mw.fwupdated[i] = true;
                        }

                        hw.dialog( "close" );
                        hwcontent.headerwindow('hideLastScreen');
                        var sc_id = $("#hw_menupage").data('last_screen');
                        if (sc_id == ('#screen_head_'+_vars[curIndex])){
                            mw.mainwindow( "issueQuery", {qo:queryObject,cb:null} );
                        }
                  },
                    Cancel : function(){
                        hw.dialog( "close" );
                        hwcontent.headerwindow('hideLastScreen');
                        $("#hw_menupage").fadeIn();
                    },
               }
            });

            //create the tabbed widget for the query options provided
            var acc_data = "";
            var tab_header = "<ul>";
            for( v in _vars ){
                tab_header += "<li> <a href=\"#tablecontainer_" + _vars[v] + "\" title='"+ _qoinfo[v] +"'>" + _vars[v] + "</a></li>";
                var filter_button = "<button id='" + _vars[v] + "_filter' style='float:left'> Filter </button>";
                var header_button = "<button id='" + _vars[v] + "_header' style='float:left'> Header </button>";
                var saveref_button = "<button id='" + _vars[v] + "_saveref' style='float:left'> Save RefFile </button>";
                var acc_datatable = "<div id='datatable_" + _vars[v] + "'> </div>";
                var table_container = "<div id='tablecontainer_" + _vars[v] + "'>" + 
                                       filter_button +
                                       header_button +
                                       saveref_button +
                                       acc_datatable +
                                       "</div>";
                acc_data += table_container;
            }
            tab_header += "</ul>";
            //function to resize the contents of the mainwindow
            var resizeContents = function(w, h){
                $( "#mainwindow" ).addClass("ui-widget-content").
                    css({
                        width:w,
                        height:h
                    });
                $( "#accordion" ).css({
                    "width": w-5,
                    "height": h-5
                    });
                $( "div[id^='tablecontainer_']" ).css( "height", h - 100);
            };

            //Handle the listing of the selected items.
            $("#check").button({
                icons:{
                    primary:"ui-icon ui-icon-circle-triangle-e"
                },
                text: false,
            }).click(function(e){
                //console.log($(this).is(":checked"), "button pressed");
                if($(this).is(":checked")){
                    var selected = $("#accordion").tabs('option', 'selected');
                    var mylist = [];
                    $("#datatable_"+_vars[selected]).datatable( "option", "getselected", mylist);
                    //console.log( mylist );
                    $("#selected_list").
                        html(mylist.toString()).
                        show();
                }else{
                    $("#selected_list").hide();
                }
            });
 
            //handle the div resize event.
            this.element.html( "<div id=\"accordion\">" + tab_header + acc_data + "</div>" ).resizable({
                resize:function( event, ui ){
                    resizeContents( ui.size.width, ui.size.height );
                }
            });
            //handle the window resize event.
            $(window).resize(function(){
                var offset =  $( "#mainwindow" ).offset();
                var h = $(window).height()-2*offset.top;
                var w = $(window).width() - 3 * offset.left;
                resizeContents( w, h );
            });
            //creating the ui elements for the data tables etc.
            for( v in _vars ){
                //set the data tables to show the data.
                $( "#datatable_" + _vars[v] ).datatable().
                bind("headerclicked", function( event, headerCellText ){ 
                    //bind the header clicked event to query with orderby option set.
                    var curIndex =$( "#accordion" ).tabs('option', 'selected'); 
                    //reset the sliders
                    $( "#scroll_" + _vars[curIndex] ).slider( "option", "value", 0 );
                    var queryObject = mw.mainwindow( "getQueryObject", curIndex );
                    queryObject.orderby = headerCellText;
                    mw.mainwindow( "issueQuery", {qo:queryObject,cb:null} );
                    //console.log( _vars[curIndex], "Header Clicked: ", headerCellText );
                });
                
                //add the sliders
                var scroll_info = $( "<span> Showing 0 - 1000 of 10000</span>" ).
                addClass( 'ui-state-highlight' ).
                attr( "id", "scrollinfo_" + _vars[v] ).
                css({
                    'text-align': 'center',
                    'width': '98%',
                    'float': 'left',
                    'padding': '1px',
                });
                var page_rows = this.page_rows;
                var scroll_bar = $( "<div></div>" ).
                attr( "id", "scroll_" + _vars[v] ).
                slider({
                    range: false,
                    min: 0,
                    max: 10000,
                    step: page_rows,
                    value: 0,
                    slide: function( event, ui ){
                        var maxvalue = $(this).slider( "option", "max" );
                        var end = Math.min( ui.value + page_rows, maxvalue );
                        var start = ui.value;
                        if(start == maxvalue){
                        	start = page_rows * Math.floor(maxvalue/page_rows); 
                        }
                        end -= 1;
                    	$( "#scrollinfo_" + $(this).attr( "id" ).split('_')[1] ).
                		html( "Show " + start + " - " + end + " of " + maxvalue );
                    },
                    stop: function( event, ui ){
                        var curIndex =$( "#accordion" ).tabs('option', 'selected'); 
                        var queryObject = mw.mainwindow( "getQueryObject", curIndex );
                        var maxVal = $(this).slider( "option", "max" );
                        var value = $(this).slider( "option", "value" );
                        var end = Math.min( value + page_rows, maxVal ); 
                        mw.mainwindow( "issueQuery", {qo:queryObject,cb:null} );
                    }
                   });
                var msg = $( "<div></div>" );
                msg.
                    css({
                        float: "left",
                        height: "20px",
                        margin: "10px",
                        width: "250px",
                    }).
                    append( scroll_bar ).
                    append( scroll_info ).
                    insertBefore( "#datatable_" + _vars[v] );

                //configure the saveref buttons. We create multiple buttons one for each tab.
                $( "#" + _vars[v] + "_saveref" ).
                    click(function(){
                        var svals = new Array();
                        //console.log( "save ref clicked" );
                        var queryOption = $(this).attr("id").split('_')[0];
                        $( "#datatable_" + queryOption ).
                            datatable( "option", "getselected", svals );
                        if( svals.length > 0 ){
                            var saveDialog = $( "<div></div>" );
                            saveDialog.
                            append('<span class="ui-icon ui-icon-info" style="float:left; margin:0 7px 50px 0;">').
                            append('<p>Specify the name of the ref file.</p>').
                            append('<input type="text" value="' + queryOption + '.ref"> </input>').
                            dialog({
                                title: "Save file",
                                buttons:{
                                    Save:function(){
                                        var filename =  saveDialog.find('input[type]="text"').val();
                                        if( filename.indexOf( '.', 0 ) < 0 ){ //the user did not specify the extension.
                                            filename = filename + '.ref';
                                        }
                                        //change the save dialog to say only ok
                                        saveDialog.html("").
                                        append("<center> <img src='css/spinner.gif'></img></center>").
                                        dialog( "option", {
                                            "buttons" : { "Ok": function() { $(this).dialog("close"); } },
                                            "width" : "400"
                                            });
                                        $.post( "getreffile", {qo:queryOption, sv:svals, fname:filename},
                                        function(data){
                                            var obj = JSON.parse(data);
                                            saveDialog.html("").
                                            append('<span class="ui-icon ui-icon-info" style="float:left; margin:0 7px 50px 0;">');
                                            if( obj.status == "success" ){
                                                saveDialog.
                                                append("<p> File Saved at: <a href=" + obj.link + " target='_blank'>"+ obj.msg + "</a></p>"); 
                                            }else{
                                                saveDialog.
                                                append("<p class='ui-state-error' " + 
                                                "style='padding=10px;margin:10px;font-face:bold;'> Error: " +
                                                obj.msg + '</p>');
                                            }
                                        });
                                    },
                                    Cancel:function(){
                                        saveDialog.dialog( "close" );
                                        saveDialog.remove();
                                        //$('body' ).remove( saveDialog );
                                    }
                                }
                            });

                            //console.log( url );
                        }else{
                            $( "#messagebox" ).html("").
                            append('<p><span class="ui-icon ui-icon-info" style="float:left; margin:0 7px 50px 0;"></span></p>').
                            append('<p> Please select at least one item before trying to save the ref file.</p>').
                            dialog( "option", {
                                title:"Info",
                                buttons:{
                                    Ok:function(){
                                        $(this).dialog( "close" );
                                    }
                                }
                            });
                            $( "#messagebox" ).dialog( "open" );
                       }
                    }).
                    button({
                        icons:{
                            primary: "ui-icon-disk"
                        },
                    });
                //we want to show save diagnosis button instead of saveref for [gain, qts, qtb]..
                if( (v >= _vars.indexOf("GAIN")) &&  (v < _vars.indexOf("GAINMOVIE"))){
                    $( "#" + _vars[v] + "_saveref" ).
                        off( 'click' ).
                        button( "option", "label", "Save DiagFile" ).
                        click(function(){
                            var svals = new Array();
                            //console.log( "save ref clicked" );
                            var queryOption = $(this).attr("id").split('_')[0];
                            $( "#datatable_" + queryOption ).
                                datatable( "option", "getselected", svals );
                            if( svals.length > 0 ){
                                var saveDialog = $( "<div></div>" );
                                saveDialog.
                                append('<span class="ui-icon ui-icon-info" style="float:left; margin:0 7px 50px 0;">').
                                append('<p>Specify the name of the diag file.</p>').
                                append('<input type="text" value="' + queryOption + '.diag"> </input>').
                                dialog({
                                    title: "Save file",
                                    buttons:{
                                        Save:function(){
                                            var filename =  saveDialog.find('input[type]="text"').val();
                                            if( filename.indexOf( '.', 0 ) < 0 ){ //the user did not specify the extension.
                                                filename = filename + '.diag';
                                            }
                                            //change the save dialog to say only ok
                                            saveDialog.html("").
                                            append("<center> <img src='css/spinner.gif'></img></center>").
                                            dialog( "option", {
                                                "buttons" : { "Ok": function() { $(this).dialog("close"); $(this).remove()} },
                                                "width" : "400"
                                                });
                                            //console.log("Qo:", queryOption, "Sv:", svals);
                                            $.post( "getdiagfile", {qo:queryOption, sv:svals, fname:filename},
                                            function(data){
                                                var obj = JSON.parse(data);
                                                saveDialog.html("").
                                                append('<span class="ui-icon ui-icon-info" style="float:left; margin:0 7px 50px 0;">');
                                                if( obj.status == "success" ){
                                                    saveDialog.
                                                    append("<p> File Saved at: " + obj.msg + "</p>"); 
                                                }else{
                                                    saveDialog.
                                                    append("<p class='ui-state-error' " + 
                                                    "style='padding=10px;margin:10px;font-face:bold;'> Error: " +
                                                    obj.msg + '</p>');
                                                }
                                            });
                                        },
                                        Cancel:function(){
                                            saveDialog.dialog( "close" );
                                            saveDialog.remove();
                                            //$('body' ).remove( saveDialog );
                                        }
                                    }
                                });

                                //console.log( url );
                            }else{
                                $( "#messagebox" ).html("").
                                append('<p><span class="ui-icon ui-icon-info" style="float:left; margin:0 7px 50px 0;"></span></p>').
                                append('<p> Please select at least one item before trying to save the diag file.</p>').
                                dialog( "option", {
                                    title:"Info",
                                    buttons:{
                                        Ok:function(){
                                            $(this).dialog( "close" );
                                        }
                                    }
                                });
                                $( "#messagebox" ).dialog( "open" );
                           }
                        });
                }
                //we want to show play button instead of saveref for gain_movie 
                if(v == _vars.indexOf("GAINMOVIE")){
                    $( "#" + _vars[v] + "_saveref" ).
                    off( 'click' ).
                    button( "option", "label", "Show Play Command" ).
                    click(function(){
                        var svals = new Array();
                        var queryOption = $(this).attr("id").split('_')[0];                        
                        $( "#datatable_" + queryOption ).
                            datatable( "option", "getselected", svals );
                        if( svals.length == 1 ){
                            var saveDialog = $( "<div></div>" );
                            saveDialog.
                            append('<span class="ui-icon ui-icon-info" style="float:left; margin:0 7px 50px 0;">').
                            append("<center> <img src='css/spinner.gif'></img></center>").
                            dialog({
                                width: '700px',
                                //height: '260px',
                                title: "Command to play Gain movie",
                                buttons:{
                                    Ok:function(){
                                        saveDialog.dialog( "close" );
                                        saveDialog.remove();
                                    }
                                }
                            });
                        	
                            $.post( "getplotmoviecommand", {sv:svals[0], },
	                            function(data){
	                                var obj = JSON.parse(data);
	                                saveDialog.html("").
	                                append('<span class="ui-icon ui-icon-info" style="float:left; margin:0 7px 50px 0;">');
	                                if( obj.status == "success" ){
	                                    saveDialog.
	                                    append("<p>" + obj.msg + "</p>"); 
	                                }else{
	                                    saveDialog.
	                                    append("<p class='ui-state-error' " + 
	                                    "style='padding=10px;margin:10px;font-face:bold;'> Error: " +
	                                    obj.msg + '</p>');
	                                }
    	                    });
                        }else{
                            $( "#messagebox" ).html("").
                            append('<p><span class="ui-icon ui-icon-info" style="float:left; margin:0 7px 50px 0;"></span></p>').
                            append('<p> Please select one (and only one) item before trying to show the play command.</p>').
                            dialog( "option", {
                                title:"Info",
                                buttons:{
                                    Ok:function(){
                                        $(this).dialog( "close" );
                                    }
                                }
                            });
                            $( "#messagebox" ).dialog( "open" );
                       }
                    });
            }
                
                //configure the filter buttons.
                $( "#" + _vars[v] + "_filter" ).
                    button({
                        icons:{
                            primary:"ui-icon-gear"
                        }
                    });

                $( "#" + _vars[v] + "_filter" ).
                    click( function(){
                        fw.dialog( "option", {title : "Filter options for " + $(this).attr( "id" ).split('_')[0]} );
                        fw.dialog( "open" );
                    });
                
                //configure the header buttons.
                $( "#" + _vars[v] + "_header" ).
                    button({
                        icons:{
                            primary:"ui-icon-gear"
                        }
                    });

                $( "#" + _vars[v] + "_header" ).
                    click( function(){
                        hw.dialog( "option", {title : "Header options"} );
                        hw.dialog( "open" );
                    });
            }//done addomg data visible in each tab.

            $( "#accordion" ).tabs({
                //force to show the first tab.
                selected: 0,
                //specifying what to do when we start showing up the tabs.
                show: function( e, ui ){
                    //if the show items check box is checked, we want to hide the prev results.
                    if($("#check").is(":checked")){
                        $("#check").trigger("click");
                    }
                    mw.mainwindow( "option", "busy", true );
                    var selectedValues = new Array();
                    //disable the special tabs if the current tab isn't MS or LDSBP
                    if( ui.tab.text == "MS" || ui.tab.text == "LDSBP" ){
                        $( "#accordion" ).tabs( "option", "disabled", [] );
                    }else if ( ui.tab.text == "GAIN" || ui.tab.text == "QBS"  || ui.tab.text == "QFS" || ui.tab.text == "QTS"){
                    	$( "#accordion" ).tabs( "option", "disabled", 
                        [_vars.indexOf("GAIN"), _vars.indexOf("QBS"), _vars.indexOf("QFS"), _vars.indexOf("QTS"), _vars.indexOf("GAINMOVIE")] );
                    }else{
                        $( "#accordion" ).tabs( "option", "disabled", 
                        [_vars.indexOf("GAIN"), _vars.indexOf("QBS"), _vars.indexOf("QFS"), _vars.indexOf("QTS")] );
                    }
                    //hide all more_opts other than the one for current tab.
                    for(var op in _vars){
                        $("#more_opts_"+_vars[op]).hide();
                    }
                    $("#more_opts_"+ui.tab.text).show();
                    //reset the sliders
                    $( "#scroll_" + ui.tab.text ).slider( "option", "value", 0 );
                    var queryObject = mw.mainwindow( "getQueryObject", ui.index );
                   //we would like to un-select any selections in the next tabs to prevent stale results.
                    for( var iVar = ui.index+1; iVar < _vars.length; iVar++ ){
                        $( "#datatable_" + _vars[iVar] ).datatable( "option", "selectall", false );
                        //reset the more opts for the tabs to the right of the current tab.
                        $( "#more_opts_"+ _vars[iVar] ).filteroptions( "reset" );
                    }
                    //get the list of selected rows in this tab. (this works for tabs with no data as well).
                    $( "#datatable_" + ui.tab.text ).datatable( "option", "getselected", selectedValues );
                    //If there are no selections in the current tab load the data from server
                    if( selectedValues.length == 0  || mw.fwupdated[ui.index] ){ 
                        //set the fw updated flag for this tab to false.
                        mw.fwupdated[ui.index] = false;
                        //SPECIAL CASE: since the GAINS, QBS, QFS, QTS are too big we dont' want to start the query for thoese 
                        if( (ui.index >= _vars.indexOf("GAIN")) && (ui.index < _vars.indexOf("GAINMOVIE"))){
                            selectedValues = new Array();
                            //we expect the pqo to be MS because we enable these special tabs only when  is selected.
                            $( "#datatable_" + queryObject.pqo ).datatable( "option", "getselected", selectedValues );
                            //console.log( "Looking at " + ui.tab.text );
                            if( selectedValues.length == 0 || selectedValues.length > 50 ){
                                $("#accordion").tabs( "select", _vars.indexOf("MS") ); //move the selection MS tab.
                                mw.mainwindow( "option", "busy", false );
                                //we need to revert to previous tab and show a msg box.
                                $( "#messagebox" ).html("").
                                append('<p><span class="ui-icon ui-icon-info" style="float:left; margin:0 7px 50px 0;"></span></p>').
                                append('<p> Make a selection in MS tab (less than 50 rows) before accessing ' + ui.tab.text + '</p>').
                                dialog( "option", {
                                    title:"Info",
                                    buttons:{
                                        Ok:function(){
                                            $(this).dialog( "close" );
                                        }
                                    }
                                });
                                $( "#messagebox" ).dialog( "open" );
                                return;
                            }
                        }
                        //console.log( queryObject );
                        mw.mainwindow( "issueQuery", {
                            qo:queryObject,
                            cb:function( obj ){
                                fwcontent.filterwindow( "option", "enable", [obj.qso, obj.qsof] )
                                mw[ui.tab.text] = [obj.qso, obj.qsof];
                            }
                        });
                    }else{//we dont' have to wait for the JSON request to complete.
                        mw.mainwindow( "option", "busy", false );
                        fwcontent.filterwindow( "option", "enable", mw[ui.tab.text] );
                    }
                }
            });

            $( "#accordion" ).css( "width", this.element.width() - 5 ).
                css( "height", this.element.height() - 5 );
           
            //add a plot gains button in the gains tab. All "Save Reffile" buttons have the "id" of format
            //"_vars[v]_saveref" so GAIN id will be GAIN_saveref use this to add plot gains button at right tab.
            var plotter_func = function(e){
                    var moduleui_name = $(this).attr("moduleui");
                    var tablename = $(this).attr("tablename");
                    var sv = [];
                    $( "#datatable_" + tablename ).datatable( "option", "getselected", sv );
                    if(sv.length == 0){
                        $( "#messagebox" ).html("").
                                append('<p><span class="ui-icon ui-icon-info" style="float:left; margin:0 7px 50px 0;"></span></p>').
                                append('<p> Select one or more ' + tablename + 'tables before plotting them. </p>').
                                dialog( "option", {
                                    title:"Info",
                                    buttons:{
                                        Ok:function(){
                                            $(this).dialog( "close" );
                                        }
                                    }
                        });
                        $( "#messagebox" ).dialog( "open" );
                        return;
                    }
                    //we use the local storage to set the options for gain plots ui in module ui.
                    if(typeof(Storage) !== "undefined"){
                        var day = new Date();
                        var filename = day.getTime() + tablename + '.diag';
                        //console.log(day.getTime(), tablename, sv, filename);
                        //Post for creating the diag file.
                        var saveDialog = $("<div></div>").
                            append("<center> <img src='css/spinner.gif'></img></center>").
                            dialog({close:function(){$(this).remove();}});
                        
                        $.ajax({
                            type: "POST",
                            url : "getdiagfile", 
                            data: {qo:tablename, 'sv':sv, fname:filename},
                            async: false, //use synchronized request to make sure that we wait for the file to generate.
                            success: function( obj ){
                                obj = JSON.parse(obj);
                                saveDialog.html("").
                                append('<span class="ui-icon ui-icon-info" style="float:left; margin:0 7px 50px 0;">');
                                if( obj.status == "success" ){
                                    saveDialog.remove();
                                    //NOTE: we specify the name of the module ui that we want lo load. so ensure that this name matches
                                    //the epected name of the module.
                                    localStorage.showmodule = JSON.stringify({"name":moduleui_name, "params":[obj.msg]});
                                }else{
                                    saveDialog.
                                    append("<p class='ui-state-error' " + 
                                    "style='padding=10px;margin:10px;font-face:bold;'> Error: " +
                                    obj.msg + '</p>');
                                }
                            }
                         });
                    }else{
                        alert("Your browser doesn't support the required features for this to work.");
                    }
            }

            for(var i = _vars.indexOf("GAIN"); i < _vars.indexOf("GAINMOVIE"); i++){
                var plotButton = $("<a href='datamanager' target='_blank'>Plot " +  _vars[i] + "</a>").
                    button().
                    attr("tablename", _vars[i]).
                    attr("moduleui", _vars[i]+"Plotter").
                    click(plotter_func).
                    insertAfter($("#" + _vars[i] + "_saveref"));
            }
        },
        
        /* creates a query object which can directly be sent as a parameter for querier.py */ 
        getQueryObject: function( curIndex ){
            var _vars = this._qmData.QUERY_OPTIONS; 
            var prevSelectedValues = new Array();
            var previousQueryOption = null;
            var iVar = 1;
            //discover the previous tab with selections for sending to a query.
            while( !prevSelectedValues.length && (curIndex-iVar) > -1){
            //see if any values were selected in table of prev tabs
                $( "#datatable_" + _vars[curIndex-iVar] ).datatable( "option", "getselected", prevSelectedValues );
                iVar += 1;
            }
            //if there are no previous selections then put null for the values.
            if( prevSelectedValues.length == 0 ){ 
                prevSelectedValues = null;
                previousQueryOption = null;
            }else{
                previousQueryOption = _vars[curIndex-iVar+1];
            }
            //console.log( this.element[_vars[curIndex]] );
            var querySelectionOptions = null;
            var names = new Array();
            var fwqso = {}
            for( var i in this._qmData.QUERY_SELECTION_OPTIONS ){
              names.push( this._qmData.QUERY_SELECTION_OPTIONS[i].name );
            }
            //console.log(names);
            //create the query selection options object to be sent.
            for( var i in names ){
              var sv = new Array();
              var numrows = 0;
              $("#filtertable_" + names[i] ).datatable( "option", "getselected", sv );
              numrows = $("#filtertable_" + names[i] ).datatable( "getNumRows", numrows );
              fwqso[names[i]] = {"sv":sv, "numrows": numrows};
            }
            //console.log( "QSO Sent: ", fwqso );
            querySelectionOptions = fwqso;
            
            var queryHeader = new Array();
            $("#headertable_" + _vars[curIndex] ).datatable( "option", "selectfirst", null );
            $("#headertable_" + _vars[curIndex] ).datatable( "option", "getselected", queryHeader );
            
            var _extras = $("#more_opts_"+_vars[curIndex]).filteroptions("getQueryObjects");
            var _offset = $( "#scroll_" + _vars[curIndex] ).slider( "option", "value" );
            var _limit = this.page_rows; 
            return {
                qo:_vars[curIndex],
                pqo:previousQueryOption,
                sv:prevSelectedValues,
                qso:querySelectionOptions,
                header:queryHeader,
                extras: _extras, 
                offset: _offset,
                limit: _limit
            };
        },
        
        /*issues the query to the querier.py and calls the callback with json object */
        issueQuery:function( val ){ //queryObject, callback ){
                var queryObject = val.qo;
                var callback = val.cb;
                var mw = this.element;
                var page_rows = this.page_rows;
                mw.mainwindow( "option", "busy", true);
                //console.log( queryObject );
                $.ajax({
                    type: "POST",
                    url : "querier", 
                    data: JSON.stringify(queryObject),
                    success: function( obj ){
                        //obj = JSON.parse(obj);
                        //set the widget width and height.
                        var tcontainer = $( "#tablecontainer_" + queryObject.qo );
                        tcontainer.height( $( "#mainwindow" ).height() - 100 );
                        $( "#datatable_" + queryObject.qo ).
                            css( "overflow", "auto" ).
                            datatable( "option", {
                                headerinfo: obj.headerinfo,
                                header: obj.header, 
                                data: obj.data,
                                width: "100%",
                                height: "100%"
                        }); 
                        //update more_opts_ to show the details of current tab.
                        $( "#more_opts_"+queryObject.qo ).filteroptions( "updateColNames", obj.extras ); 
                        //update the slider maxvalue
                        var cur_slider =  $( "#scroll_" + queryObject.qo );
                        cur_slider.slider( "option", "max", obj.maxrows );
                        var maxVal = cur_slider.slider( "option", "max" );
                        var value = cur_slider.slider( "option", "value" );
                        var end = Math.min( value + page_rows, maxVal ); 
                        $( "#scrollinfo_" + queryObject.qo ).
                            html( "Showing " + obj.initrow + " - " + obj.endrow + " of " + maxVal );
                        //unblock the ui.
                        mw.mainwindow( "option", "busy", false );
                        //show an error message when something goes wrong on the server side.
	                    if(obj.status == "error"){
		                    var errDialog = $("<div></div>");
		          		  	errDialog.
		                    append('<span class="ui-icon ui-icon-info" style="float:left; margin:0 7px 50px 0;">').
		                    append('<p>' + obj.msg + '</p>').                                 
		                    dialog({
		                  	  width:300,
		                  	  height:200,
		                  	  buttons:{
		                  		  Ok:function(){
		                  			  $(this).dialog("close");
		                  			  $(this).remove();
		                  		  }
		                  	  }
		                    });
	                    }
                        if( callback ){
                        	callback( obj );             
                        }
                },
                dataType:'JSON',
                contentType:'application/json'
              }); 
        },
			
        _setBusy: function( val ){
            if(val){
                this.element.block({
                    css: { 
                        border: 'none', 
                        padding: '15px', 
                        backgroundColor: '#000', 
                        '-webkit-border-radius': '10px', 
                        '-moz-border-radius': '10px', 
                        opacity: .5, 
                        color: '#fff' 
                    },
                    message: "<h3> Loading... </h3>"
                });
            }else{
                this.element.unblock();
            }
        },

		destroy: function() {			
//			this.element.html( "" );
		},
	
        _setOptions: function(){
            $.Widget.prototype._setOptions.apply(this, arguments);
        },

		_setOption: function( option, value ) {
			$.Widget.prototype._setOption.apply( this, arguments );
            switch(option){
                case "init":
                    this._qmData = value;
                    this.setUI();
                    //this._setBusy( false );
                    break;
                case "busy":
                    this._setBusy( value );
                    break;
            }
        }
	});
})( jQuery );
