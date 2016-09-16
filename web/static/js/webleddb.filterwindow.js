/* The idea here is to create <div><div> elements for each Query Selection Option and call it screen_qso.
   The main menu <div></div> will contain buttons which will show the corresponding <div> element which
   are hidden by default. */
(function($) {
    $.widget("webleddb.filterwindow", {
		options: {
            init: null,
            enable: null 
		},
				
		_create: function() {
            this.menu_items = new Array();
            this.content_holder = $( "<div></div>" );
            this.element.
                css({
                    overflow:'hidden',
                    width:"300px",
                    height:"400px"
                }).
                append( this.content_holder );
		},
        
	    /* set up the ui for filter window. Create a menu page with a list of buttons. 
           make them show a screen of options corresponding to the button clicked. */
        _fillData: function( data ){
            var qso = data.qso;
            this.content_holder.css( "width", 300 );

            //fill the first page of the filter window with buttons.
            var menu_content = $( "<div></div>" );
            menu_content.
                css({
                        float: "left",
                        width: "300px",
                        height: "400px",
                }).attr("id", "fw_menupage");
            this.menu_items = new Array();
            
            for( var i in qso ){
                var item = $( "<button>" + qso[i].name + "</button>" );
                this.menu_items.push( item );
                //we will set the attribute "sa" of the button which is not standard to be able to scroll the right amount.
                item.
                attr({
                    "screen": "#screen_" + qso[i].name,
                    "id": "menu_" + qso[i].name
                    }).
                css({
                    width: "300px",
                    height: "30px"
                }).
                click( function(){
                    $( $(this).attr( "screen" ) ).show( "slide", {}, 500, function(){
                        menu_content.fadeOut(); //slide the menu_content away. 
                    });
                }).
                button();
                menu_content.append( item );
                menu_content.append( "<br/>" );
            }
            //Add the column filter options to the menu page (one per screen).
            var arr = data.qo; 
            for(var v in arr){
                var tmp = $( "<div></div>" ); 
                tmp.
                hide().
                attr("id", "more_opts_" + arr[v]).
                filteroptions();
                menu_content.append(tmp);
            }

            this.content_holder.append( menu_content );

            //now start filling the rest of the pages for filter window
            for( var i in qso ){
                var _button = $( "<button>Back</button>" );
                _button.
                    css( "float", "right").
                    attr( "screen", "#screen_" + qso[i].name ).
                    click( function(){
                        var sc_id = $(this).attr( "screen" );
                        menu_content.fadeIn();
                        $( sc_id ).hide( "slide", {}, 500);    
                    }).
                    button();
                var content = $( "<div> </div>" );
                var span_element = $( "<span> </span>" ).
                    css({
                        "text-align" : "center",
                        "float" : "left",
                        "width" : "240px",
                        "height": "30px"
                    }).addClass( 'ui-state-highlight ui-corner-all' ).
                    append( qso[i].helptext );
                content.
                    append( span_element ).
                    attr( "id", "screen_" + qso[i].name ).
                    append( _button ).
                    css({
                        float: "left",
                        width: "300px",
                        height: "400px",
                        position: "absolute",
                        left: "10px",
                        top: "10px",
                        display: "none",
                        "background-color": "#ffffff"
                    });
                var qso_data = $( "<div></div>" );
                qso_data.
                attr( "id", "filtertable_" + qso[i].name ).
                datatable().
                    datatable( "option", {
                        headerinfo: qso[i].header,
                        header: qso[i].header,
                        data: qso[i].data,
                        "selectall": qso[i].defaultin
                }).bind("headerclicked", function(event, ui){
                    $(this).datatable("sortData", ui);
                });
                qso_data.css({
                    position: "relative",
                    top: "5px",
                    width: "295px",
                    height: "350px",
                    overflow: "auto"
                });
                content.append( qso_data );
                this.content_holder.append( content );
            }
        },

        /* Function to enable partial Qso based on the Qo. This is filled
           in whenever there is a query to the server (querier sends this info). */
        _enableItems: function( value ){
        	var qso = value[0];
        	var qsof = value[1];
            for( var i in this.menu_items ){
                this.menu_items[i].button( "option", { disabled:true, icons: {primary:''} } );
            }
            for( var i in qso ){
                $( "#menu_" + qso[i] ).button( "option", { disabled:false } );
            }
            for( var i in qsof ){
            	$( "#menu_" + qsof[i] ).button( "option", { disabled:false, icons: {primary: 'ui-icon-alert'} } );
            }
            //hide the screens and show the menu screen.
            this.element.find( 'div[id^="screen_"]' ).each(function(){
                $(this).hide();
            });
            this.element.find( 'div[id="fw_menupage"]' ).show(); 
        },
        _setOptions: function(){
            $.Widget.prototype._setOptions.apply(this, arguments);
        },

		_setOption: function( option, value ) {
			$.Widget.prototype._setOption.apply( this, arguments );
            switch(option){
                case "init":
                    this._fillData( value );
                    break;
                case "enable":
                    //enable these buttons only and disable rest of the buttons.
                    this._enableItems( value );
                    break;
            }
        }
	});
})( jQuery );
