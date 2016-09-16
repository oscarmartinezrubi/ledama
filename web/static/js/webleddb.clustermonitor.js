var collection = null; //holds all the nodes ui.
var overlay = null;
var curSelection = "cpu"; //set what the ui shows by default. also depends on what is shown in nodestat.js
var refreshInterval = 20; //interval for the autorefresh
var refreshCounter = 0;

function fillData(){
    var uname = $("#username").val();
    var cmd = $("#command").val();
    $.ajax({
        'url':"clustermonitor",
        'data':{'uname':uname, 'cmd':cmd}, //getJSON parameters to server
        'success': function( _data ){
            //populate these inputs whenever the source changes.
            $("#username").autocomplete("option", "source", _data.users);
            $("#command").autocomplete("option", "source", _data.cmds);
            for( var i = 0; i < _data.nodes.length; i++ ){
                if (_data.nodes[i].nodeid.substring(0, 4) != "node"){
                    $( collection[i] ).storagenodestat( "option", _data.nodes[i] );
                }else{
                    $( collection[i] ).nodestat( "option", _data.nodes[i] );
                }
            }
            overlay.hide("fade", 1000);
        },
        'dataType':'JSON'
    });//getJSON
}

function refresh(){
	$( "#autoreftext" ).html("Auto Refresh (" + (refreshInterval-refreshCounter) + "):");
	if (refreshCounter == 0){
		overlay.show("fade", 1000);
		fillData();
	}
	refreshCounter++;
	if (refreshCounter == refreshInterval){
		refreshCounter = 0;
	}
}

/* Show or Hide the panels on the nodestat ui based on the 
curSelection value */
function toggleInfoPanel(){
    var id = $(this).attr("id");
    if( id != curSelection ){
        if( collection ){
            collection.each( function(){
                 if ($(this).hasClass('storagenodestat')){
                     $(this).storagenodestat( "switchView", {_show:id, _hide:curSelection} );
                 }else{
                     $(this).nodestat( "switchView", {_show:id, _hide:curSelection} );
                 }
            });
        }
        curSelection = id;
    }
}

function startUI(){

    //show the spinner as a dialogbox.
    var spinnerimg = $( "#spinnerimg" );
    overlay = $( "<div></div>" );
    overlay.append(spinnerimg).
        addClass( 'ui-widget-overlay' );
    $( 'body' ).append( overlay );                  
    spinnerimg.css({
            'position': 'absolute',
            'left': $(window).width()/2,
            'top': $(window).height()/2
            }).addClass( 'ui-widget-content').show();


    $(function(){
            var timer_handle = null;
            var timer_text_handle = null;
            var autocomplete_search = function(){
                $(this).autocomplete( "search", "" );
            };

            var cmd_entry = $( "<input id='command'> </input>" ).
                bind( "click", autocomplete_search ).
                css( "width", "100px" );
            var cmd = $( "<div>Command: </div>" ).
                append( cmd_entry ).
                css({
                "margin-left" : "5px",
                "float": "right" });

            var uname_entry = $( "<input id='username'> </input>" ).
                bind( "click", autocomplete_search ).
                css( "width", "100px" );
            var uname = $( "<div>User: </div>" ).
                append( uname_entry ).
                css({
                    "float": "right" 
                    });
            
            //add the username and command entry ui to the controlpanel div.
            $( "#controlpanel" ).
                append(cmd, uname).
                css( "width", "750px" );

            $( "#radio" ).buttonset();
            $( "#autoon" ).click(function(){
            	if (timer_handle != null){
            	    timer_handle = clearInterval( timer_handle );
            	}
                refreshCounter = 0;
                refresh();
                timer_handle = setInterval( "refresh();", 1000 );
            });

            $( "#autooff" ).click(function(){
                timer_handle = clearInterval( timer_handle );
                $( "#autoreftext" ).html("Auto Refresh:");
            });

            $( "#info" ).buttonset();
            $( "#cpu" ).click( toggleInfoPanel );
            $( "#disk" ).click( toggleInfoPanel );
            $( "#net" ).click( toggleInfoPanel );

            /*
            make request to the clustermonitor on the server to get the 
            info about the nodes. Expect a json object which contains all the info.
            */
            $.ajax({
                'url': "clustermonitor", 
                'success':function( _data ) {
	                overlay.hide( 'fade', 1000 );
	                //create the autocomplete boxes with the usernames and commands
	                $("#username").autocomplete({
	                    source:_data.users,
	                    minLength:0,
	                    //refresh the node stats and set the cmd to "" when user name is changed. 
	                    select:function(e, ui){
	                        if(ui.item){
	                            uname_entry.val(ui.item.value);
	                        }
	                        cmd_entry.val("");
	                        overlay.show("fade", 1000);
	                        fillData();
	                    },
	                });
	                $("#command").autocomplete({
	                    source:_data.cmds,
	                    minLength:0,
	                    //refresh the node stats when the command is selected. 
	                    select:function(e, ui){
	                        overlay.show("fade", 1000);
	                        if(ui.item){
	                            cmd_entry.val(ui.item.value);
	                        }
	                        fillData();
	                    }
	                });
	                var firstStNode = true
	                for( var i = 0; i < _data.nodes.length; i++ ){
		                var node = $( "<div></div>" );
		                if (_data.nodes[i].nodeid.substring(0, 4) != "node"){
		                	node.storagenodestat( _data.nodes[i] );
		                	if (firstStNode){
		                		firstStNode = false;
		                		var dummy = $( "<div></div>" );
		                		dummy.css('clear','both')
		                		$( '#nodes' ).append(dummy);
		                	}
			                $( '#nodes' ).append( node );
		                }else{
			                node.nodestat( _data.nodes[i] );
			                $( '#nodes' ).append( node );
			                //when a node is clicked we issue a query. "nodeclicked" is the even triggered from "nodestat.js" when cpu ui is clicked.
			                node.bind( "nodeclicked", function(event, ui){
			                    overlay.show( 'fade', 1000 );
			                    //start a request to server for info about the node usage.
			                    $.ajax({
			                        'url': "clustermonitor", 
			                        'data': {node:ui.id, type:ui.type}, 
			                        'success': function(obj){
			                        overlay.hide( 'fade', 1000,
			                        function(){ //called once the overlay is hidden.
			                            var msgbox = $("#messagebox").html("");
			                            if( obj ){
			                                var table = $( "<div></div>" ).datatable();
			                                table.datatable( "option", "selectable", false );
			                                table.datatable( "option", obj );
			                                table.bind( "headerclicked", function( event, ui ){
			                                    table.datatable( "sortData", ui );
			                                    });
			                                table.find(".ui-big-pad").wrapInner("<pre style='margin:0px;padding:0px;font-size:10px;'/>");
			                                msgbox.append( table ).
			                                dialog({title:ui.id, show:'slide', hide:'fade', width:700,height:window.innerHeight,modal:true});
			                            }else{
			                                var error_msg = $( "<p>Could not retrieve data </p>" ).
			                                addClass( 'ui-state-error' ).
			                                css({
			                                    'padding': '10px',
			                                    });
			                                msgbox.append( error_msg ).
			                                dialog({title:ui.id, show:'slide', hide:'fade', width:400,height:300,modal:true});
			                            }
			
			                        });///overlay.hide
			                    },
			                    'dataType':'JSON'
			                    });//getJSON (node usage info now)
			                });
		                }
	                }
	                collection = $( ".nodestat,.storagenodestat" ); 
	                //var last_node = collection.last();
	                //var pos = last_node.position();
	                //overlay.height( pos.top + 2*last_node.height() );
            },
            'dataType':'JSON'
            });
    });
}
window.onload = startUI;

