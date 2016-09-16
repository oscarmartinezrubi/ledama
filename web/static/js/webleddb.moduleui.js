(function($) {
    $.widget( "webleddb.moduleui", {
		options: {
            posturl: "getcommand",
            moduleinfo: [],
            getParamsObj:null,
            issueQuery:null,
    	},
				
		_create: function() {
            this._refresh();
    	},

        issueQuery: function(val){
            var obj = val.qo;
            var callback = val.cb;

                    //post request to get the command.
                    $.ajax({
                    type: "POST",
                    url : this.options.posturl, 
                    data: JSON.stringify( obj ),
                    success: function( data ){
                        if(callback){
                            callback(data);
                        }
                    },
                    dataType:'JSON', //specify that we want to recieve JSON object
                    contentType:'application/json' //specify that we are sending JSON object.
                    }); 

        },
        getParamsObj: function(){
             var obj = {};
             var args = [];
             var parameters = this.options.moduleinfo.OPTIONS;
             obj.MODULE = this.options.moduleinfo.MODULE;
             //create a querystring for the post url and post it. disable buttons, show loading sign.
             for(var i in parameters){
                 var value =  $("#param_"+i).attr("value");
                 if(parameters[i].TYPE == "BOOLEAN"){
                     var ischecked = $("#param_"+i).attr('checked')?true:false;
                     if(ischecked){
                         value = true;
                     }else{
                         value = false;
                     }
                 }
                 args.push(value);
             }
             obj.ARGS = args;
             return obj;
        },

        _refresh: function() {
            var parameters = this.options.moduleinfo.OPTIONS;
            //clear the old ui elements.
            this.element.html("");
            //populate with the new ui elements
            this.ui_table = $("<table></table>").width("100%");
            this.tip = $("<div></div>").
                addClass("ui-state-highlight ui-corner-all");
            var self_ref = this;
            for( var parameter in parameters ){
                var ui_tr = $("<tr></tr>");
                var td_name = $("<td></td>");
                var td_value = $("<td></td>");
                var ui_value = $("<input type='text'> </input>");
                var ui_parameter = parameters[parameter];
                //show the parametername
                td_name.append( ui_parameter.NAME );
                //decide the input options to be shown in the next td.
                if(ui_parameter.TYPE == "BOOLEAN"){
                    ui_value = $("<input type='checkbox'> </input>");
                }
                if(ui_parameter.TYPE == "CHOICE"){
                    ui_value = $("<select></select>");
                    var choices = ui_parameter.DEFAULT;
                    for(var choice in choices){
                        ui_value.append("<option value='"+ choices[choice] +"'>" + choices[choice] +"</option>");
                    }
                    ui_value.css("width", "150px");
                }
                if(ui_parameter.TYPE == "TEXT"){
                    ui_value.attr("value", ui_parameter.DEFAULT);
                }
                ui_value.
                    focus(function(){
                        //when the user clicks on an input option, we show a tool tip with the help msg.
                        var pos = $(this).position();
                        self_ref.tip.
                            html('<span class="ui-icon ui-icon-info" style="float: left; margin-right: .3em;"></span>').
                            remove().
                            append($(this).attr("help")).
                            css({
                                position:"absolute",
                                top: pos.top,
                                left:pos.left + $(this).parent().width() + 5, //+5 because we use 5px padding
                                width: "200px",
                                'min-height': "50px",
                                padding: "5px",
                            });
                        self_ref.tip.insertBefore($(this));
                    }).
                    blur(function(){
                        self_ref.tip.remove();
                    }).
                    attr("help", ui_parameter.HELP).
                    attr("id", "param_" + parameter);

                td_value.append( ui_value );
                //add the td to the row.
                ui_tr.append(td_name, td_value);
                //add the row to the table.
                this.ui_table.append(ui_tr);
            }
            //configure the buttons to operate on the parameters.
            this.getcmd = $("<button>Show Command</button>").
                css('float', 'right').
                button().
                click(function(){
                    var obj = self_ref.getParamsObj();
                    //console.log( obj );
                    //disable the buttons while the query is executed.
                    $(this).attr("disabled", true);
                    self_ref.cancel.attr("disabled", true);
                    self_ref.issueQuery({
                        'qo':obj,
                        'cb':function(res){
                        //console.log( res );
                        //enable the buttons.
                        self_ref.getcmd.attr( "disabled", false );
                        self_ref.cancel.attr( "disabled", false );
                        //show the response.
                        var msgbox = $( "<div></div>" );
                        var msgbox_content = $( "<p></p>" ).
                            css( {"margin":"2px", "padding":"2px"} ).
                            append( res.msg );
                        if( res.status == "error" ){
                            msgbox_content.addClass("ui-state-error");
                        }
                        msgbox.
                        append( msgbox_content ).
                        dialog({
                            width:400,
                            height:200,
                            buttons:{
                                Ok:function(){
                                    $(this).dialog("close");
                                    $(this).remove();
                                }
                            }
                        });
                    }//callback
                    }); //issueQuery
                });

            //close button.
            this.cancel = $("<button>Cancel</button>").
                button().
                click(function(){
                    self_ref.destroy();
                });

            //add to script.
            this.add2script = $("<button>Add to Script</button>").
                button().
                click(function(){
                var obj = self_ref.getParamsObj();
                self_ref.issueQuery({
                    'qo':obj,
                    'cb':function(res){
                        if(res.status != "error"){
                            //ask the script container to add this command to the current script.
                            $("#script_container").script("addCommand", {
                                'comment':self_ref.options.moduleinfo.MODULE,
                                'command':res.msg
                                });
                        }else{
                            //show error msg box when there is an error.
                            var msgbox = $( "<div></div>" );
                            var msgbox_content = $( "<p></p>" ).
                                css( {"margin":"2px", "padding":"2px"} ).
                                append( res.msg ).
                                addClass("ui-state-error");
                            msgbox.
                            append( msgbox_content ).
                            dialog({
                                width:400,
                                height:200,
                                buttons:{
                                    Ok:function(){
                                        $(this).dialog("close");
                                        $(this).remove();
                                    }
                                }
                            });
                        }
                    }
                });
            });

            //create a temporary row for the buttons in the table.
            var tmp_tr = $("<tr></tr>");
            var tmp_td1 = $("<td></td>").
                append(this.getcmd);
            var tmp_td2 = $("<td></td>").
                append(this.add2script, this.cancel);
                tmp_tr.append(tmp_td1, tmp_td2);// this.add2script, this.cancel);
            this.ui_table.append(tmp_tr).
                resizable({
                    resize: function(event, ui){
                        var w = tmp_td2.innerWidth(); 
                        $(this).find('input[type=text]').each(function(){
                            $(this).width(w-15); //TODO: try to find the exact value to put here
                        });
                    }
                });
            
            //add the table to the moduleui div.
            this.element.append(this.ui_table);
        },

 		destroy: function() {			
	  		this.element.html( "" );
		  },

        _setOptions: function(){
            $.Widget.prototype._setOptions.apply(this, arguments);
            this._refresh();
        },

		_setOption: function( option, value ) {
			$.Widget.prototype._setOption.apply( this, arguments );
            this._refresh();
		}
	});
})( jQuery );


