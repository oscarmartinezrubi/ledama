/* The idea here is to create <div><div> elements for each Query Option and call it screen_qo.
   The main menu <div></div> will contain buttons which will show the corresponding <div> element which
   are hidden by default. */
(function($) {
    $.widget("webleddb.headerwindow", {
        options: {
            init: null,
            enable: null 
	},
				
	_create: function() {
            this.content_holder = $( "<div></div>" );
            this.element.
                css({
                    overflow:'hidden',
                    width:"300px",
                    height:"400px"
                }).
                append( this.content_holder );
	},
        
	    /* set up the ui for header window. Create a menu page with a list of buttons. 
           make them show a screen of options corresponding to the button clicked. */
        _fillData: function( data ){
            var qohead = data.qohead
            //console.log( "Headers: ", qohead );
            this.content_holder.css( "width", 400 );
            menu_content = $( "<div></div>" );	
            //fill the first page of the header window with buttons.
            menu_content.
                css({
                        float: "left",
                        width: "300px",
                        height: "400px",
                }).attr("id", "hw_menupage");
            
            for( var i in qohead ){
                var item = $( "<button>" + qohead[i].name + "</button>" );
                //we will set the attribute "sa" of the button which is not standard to be able to scroll the right amount.
                item.
                attr("screen","#screen_head_" + qohead[i].name).
                css({
                    width: "300px",
                    height: "30px"
                }).
                click( function(){
                    sc_id = $(this).attr( "screen" )
                    menu_content.data("last_screen",sc_id)
                    $(sc_id).show( "slide", {}, 500, function(){
                        menu_content.fadeOut(); //slide the menu_content away. 
                    });
                }).
                button();
                menu_content.append( item );
                menu_content.append( "<br/>" );
            }

            this.content_holder.append( menu_content );

            //now start filling the rest of the pages for header window
            for( var i in qohead ){
                var _button = $( "<button>Back</button>" );
                _button.
                    css( "float", "right").
                    attr( "screen", "#screen_head_" + qohead[i].name ).
                    click(this.hideLastScreen).button();
                var content = $( "<div> </div>" );
                var span_element = $( "<span> </span>" ).
                    css({
                        "text-align" : "center",
                        "float" : "left",
                        "width" : "240px",
                        "height": "30px"
                    }).addClass( 'ui-state-highlight ui-corner-all' ).
                    append( qohead[i].helptext );
                content.
                    append( span_element ).
                    attr( "id", "screen_head_" + qohead[i].name ).
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
                var qo_data = $( "<div></div>" );
                qo_data.
                attr( "id", "headertable_" + qohead[i].name ).
                datatable().
                    datatable( "option", {
                        headerinfo: qohead[i].header,
                        header: qohead[i].header,
                        data: qohead[i].data,
                        //"selectall": qohead[i].defaultin
                        "selectsome": qohead[i].defhead
                });
                qo_data.css({
                    position: "relative",
                    top: "5px",
                    width: "295px",
                    height: "350px",
                    overflow: "auto"
                });
                content.append( qo_data );
                this.content_holder.append( content );
            }
        },

        hideLastScreen: function(){
           //var sc_id = $(this).attr( "screen" );
           $("#hw_menupage").fadeIn();
           var sc_id = $("#hw_menupage").data('last_screen');
           $( sc_id ).hide( "slide", {}, 500);
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
            }
        }
    });
})( jQuery );
