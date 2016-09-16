function getModuleUI(module){
    //console.log(ui.item.value);
    $("#spinnerimg").show('fade');
    $("#moduleui").hide("slide");
    //get the moduleui info from server.
    $.ajax({
        'url': "initmoduleui", 
        'data': {moduleName:module},
        'success':function(data){
            $("#spinnerimg").hide('fade');
            $("#moduleui").html(""); //clear the prev ui.
            $("#moduleui").moduleui({"moduleinfo": data});
            $("#moduleui").show("slide");
            $("#script_container").dialog("open");
            //update the textbox's value to represent the ui being shown.
            //this is done by default when you select from the combo box, but not when you click
            //the buttons in the full menu display. So we set it explicitly here.
            $("#menu_item").attr("value", module);
            if(LAUNCH_PARAMS){
                for( param in LAUNCH_PARAMS.params ){
                    $("#param_"+param).val(LAUNCH_PARAMS.params[param]);
                }
                //add button for plotting
                var show_plot = $("<button>Show Plot</button>").
                    button().
                    click(function(){
                        //console.log("show button");
                        var args = $("#moduleui").moduleui("getParamsObj");
                        var dialog_box = $("<div></div>").
                            append( '<center><br/><br/><img src="css/spinner.gif" id="spinnerimg"></img></center>' ).
                            dialog();
                        //post request to get the command.
                        $.ajax({
                            type: "POST",
                            url : "plot", 
                            data: JSON.stringify( args ),
                            success: function( data ){
                                var res = JSON.parse( data );
                                //console.log( res.msg );
                                    dialog_box.
                                    dialog("close").html("").
                                    append('<a target="_blank" href="' + res.msg + '"><img src="' + res.msg + '" style="width:800px;height:320px;"></img></a>').
                                    dialog({
                                    width:"820",
//                                    height:"340px",
                                    close:function(){
                                        $(this).remove();
                                    }});
                            },
                            contentType:'application/json'
                        }); 
                    }).insertBefore($("#moduleui").find("button").first());
                //reset the LAUNCH_PARAMS variable.
                LAUNCH_PARAMS = null;
            }
        },
        'dataType':'JSON'
        });
}


/* widget to display the module ui's under different categories. */
(function($) {
    $.widget( "webleddb.blockui", {
		options: {
            //we assume that the blockinfo is set while creating the blocks.
            blockinfo:{'name':'BLOCK NAME', 'modules':[]}
    	},
				
		_create: function() {
            var bi = this.options.blockinfo;
            this.header = $("<div></div>");
            this.element.css({
                'padding':'2px',
                'float':'left',
                'width':'150px'
            });

            //set the color, contents of the header for the block of modules.
            this.header.css({
                'height':'30px',
                'width':'150px'
            }).
            append(bi.name).
            addClass('ui-state-active');
            this.element.append(this.header);

            //create a button for each module in the block.
            for(module in bi.modules){
                var modbutton = $("<button>"+ bi.modules[module] +"</button>").
                    css("width", "150px").
                    attr("value", bi.modules[module]).
                    button().
                    click(function(){
                        getModuleUI($(this).attr("value"));
                    });
                this.element.append(modbutton);
            }
    	},

        //default methods for widgets in jqui.
 		destroy: function() {			
	  		this.element.html( "" );
		  },

        _setOptions: function(){
            $.Widget.prototype._setOptions.apply(this, arguments);
        },

		_setOption: function( option, value ) {
			$.Widget.prototype._setOption.apply( this, arguments );
		}
	});
})( jQuery );

/*widget for handling the scrpting needs for the datamanager*/
(function($) {
    $.widget( "webleddb.script", {
		options: {
            addCommand:null,
            getScriptText:null,
    	},
				
		_create: function() {
                var self_ref = this; //useful for buttons callback.
                var script_header = $("<div></div>").css({
                    width: '400px',
                    height: '20px',
                }).addClass('ui-widget-header').
                append('<font size="2">Script</font>');

                this.saveButton = $('<span class="ui-corner-all ui-icon ui-icon-disk" style="float:right;border:1px solid white; margin-right: .3em;"></span>').
                mouseenter( function(){
                    $(this).css( "cursor", "pointer" );
                }).mouseleave( function(){
                    $(this).css( "cursor", "auto" );
                }).
                    css("float", "right").
                    click(function(){
                        //console.log( self_ref.getScriptText() );
                        //send the script contents to the server. save to a file name.
                            var saveDialog = $( "<div></div>" );
                            saveDialog.
                            append('<span class="ui-icon ui-icon-info" style="float:left; margin:0 7px 50px 0;">').
                            append('<p>Specify the name of the script file.</p>').
                            append('<input type="text" value=untitled.sh> </input>').
                            dialog({
                                title: "Save file",
                                buttons:{
                                    Save:function(){
                                        var filename =  saveDialog.find('input[type]="text"').val();
                                        if( filename.indexOf( '.', 0 ) < 0 ){ //the user did not specify the extension.
                                            filename = filename + '.sh';
                                        }
                                        //change the save dialog to say only ok
                                        saveDialog.html("").
                                        append("<center> <img src='css/spinner.gif'></img></center>").
                                        dialog( "option", {
                                            "buttons" : { "Ok": function() { $(this).dialog("close"); } },
                                            "width" : "400"
                                            });
                                        $.post( "savescript", {'fname':filename, 'script':self_ref.getScriptText()},
                                        function(data){
                                            //console.log(data);
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
                                    }
                                }
                            });
                    });

                this.clearButton = $('<span class="ui-corner-all ui-icon ui-icon-trash" style="float:right;border:1px solid white; margin-right: .3em;"></span>').
                mouseenter( function(){
                    $(this).css( "cursor", "pointer" );
                }).mouseleave( function(){
                    $(this).css( "cursor", "auto" );
                }).
                    css("float", "right").
                    click(function(){
                        self_ref.script.find("pre").each(function(){
                            $(this).remove();
                        });
                        //console.log("Clear Clicked!!");
                    });

                this.runButton = $('<span class="ui-corner-all ui-icon ui-icon-play" style="float:right;border:1px solid white; margin-right: .3em;"></span>').
                mouseenter( function(){
                    $(this).css( "cursor", "pointer" );
                }).mouseleave( function(){
                    $(this).css( "cursor", "auto" );
                }).
                    click(function(){
                        //console.log("Run Clicked");
                        //save and run the script
                    });

                this.script = $("<div></div>").
                    css({
                        "font-size":"10px",
                        "padding": "5px",
                        "overflow": "auto"
                    });
                script_header.append(this.clearButton, this.saveButton);// this.runButton);

                this.element.css({
                    'width':'400px',
                    'float':'right'
                }).append(script_header, this.script);
    	},

        //adds the provided command to the script element. 
        addCommand: function( value ){
            //console.log( value );
            var script_sniplet = $("<pre></pre>");
            var xmark = $('<span class="ui-icon ui-icon-trash" style="float:right; margin-right: .3em;"></span>').
                mouseenter( function(){
                    $(this).css( "cursor", "pointer" );
                }).mouseleave( function(){
                    $(this).css( "cursor", "auto" );
                }).click(function(){
                    script_sniplet.remove();
                });
            script_sniplet.
                append( xmark ).
                append( "#" + value.comment + "\n" + value.command + "\n" );
            this.script.append( script_sniplet ); 
        },
       
        //extracts all the commands added to the script.
        getScriptText: function(){
            var txt = "";
            var collection = this.script.find("pre");
            collection.each(function(){
                txt += $(this).text();
            });
            return txt;
        },

        //default methods for widgets in jqui.
 		destroy: function() {			
	  		this.element.html( "" );
		  },

        _setOptions: function(){
            $.Widget.prototype._setOptions.apply(this, arguments);
        },

		_setOption: function( option, value ) {
			$.Widget.prototype._setOption.apply( this, arguments );
		}
	});
})( jQuery );

