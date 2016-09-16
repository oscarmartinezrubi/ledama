function checkboxHandler( cb, e ){
    var th = cb.parent();
    var tr = th.parent();
    var table = tr.parent();
    if( $( cb ).attr( 'checked' ) ){
        tr.addClass( "ui-state-active" );
        table.find( "tr" ).each( function(){
                $(this).addClass( "ui-state-active" );
                });
    }else{
        table.find( "tr" ).each( function(){
                $(this).removeClass( "ui-state-active" );
                });
        tr.removeClass( "ui-state-active" );
    }
}

/* This is the widget that will be handling the mouse drag ui on the 
   data table. 
 */
(function($) {
    $.widget( "webleddb.uitable", $.ui.mouse, {
        options: {
        },

        _create: function(){
            this._mouseInit();
            //the "th" elements are also contained in a "tr" so we avoid the first row.
            var rows = this.element.find( "tr:gt(0)" );
            rows.each( function(){
                    $( this ).bind( 'mousedown', function(){
                            $( this ).toggleClass( "ui-state-active" );
                        }
                    );
            });
            this.helper = $( "<div class='ui-selectable-helper'> </div>" );
        },

        _mouseStart: function( event ){
            this.opos = {x:event.pageX, y:event.pageY};
            this.helper.css({
                "left": event.pageX,
                "top": event.pageY,
                "width":0,
                "height":0
            }); 
            $( 'body' ).append( this.helper );
            //since we start dragging from here. register the callback for adding class
            var rows = this.element.find( "tr" );
            rows.each( function(){
                $(this).bind( 'mouseover', function(){
                    $(this).addClass( 'ui-state-active' );
                });
            });
        },

        _mouseDrag: function( event ){
            var x1 = this.opos.x, y1 = this.opos.y, x2 = event.pageX, y2 = event.pageY;
            if (x1 > x2) { var tmp = x2; x2 = x1; x1 = tmp; }
            if (y1 > y2) { var tmp = y2; y2 = y1; y1 = tmp; }
            this.helper.css({left: x1, top: y1, width: x2-x1, height: y2-y1});
        },
   	   
        _mouseStop: function( event ){
            this.helper.remove();
            //since we stop dragging here. un-register the mouseover callback we added in mouseStart.
            var rows = this.element.find( "tr" );
            rows.each( function(){
                $(this).unbind( 'mouseover' );
            });
        },

        _setOptions: function(){
            $.Widget.prototype._setOptions.apply(this, arguments);
        },

		_setOption: function( option, value ) {
			$.Widget.prototype._setOption.apply( this, arguments );
	    }		
    });
})( jQuery );


/* This is the basic interface that is used by the user for creating a data table */
(function($) {
    $.widget( "webleddb.datatable", {
		options: {
      		getNumRows: null,
            sortData:null,
            sortOrder: "asc", //keeps track of the sort order of the data in table.
            width: "320",
            height: "80px",
            keyindex: 0, //index of value to keep track of for selected items.
            header: ['First', 'Second'],
            data: [],
            headerinfo: ['asdf', 'asdf'],
            selectable : true
		},
				
		_create: function() {
            this.element.addClass("ui-widget-content");
		},

        /* Sorting the rows in the table based on the colum index. */
        sortData: function( col, order ){
            if( !$.isNumeric(col) ){
                col = this.options.header.indexOf( col );
            }
            //toggle the sort order every time there is a call to sortData.
            if( this.sortOrder == "asc" ){
                this.sortOrder = "dsc";
            }else{
                this.sortOrder = "asc";
            }
            //console.log( this.sortOrder );
            var data = this.options.data;
            function sort( s, e ){
                var p = e; 
                if( p == s ){
                    return;
                }
                else
                {
                    var prow = data[p];
                    for(var i = s; i < e; i++){
                        var cur_row = data[i];
                        if( cur_row[col] < prow[col]){ 
                            prow = data[i];
                            data[i] = data[p];
                            data[p] = prow;
                        } 
                    } 
                    sort(s, e-1);
                }   
            }
            sort(0, data.length-1);
            //just reverse the sorted data if the sortOrder is different.
            if( this.sortOrder == "dsc" ){
                this.fillData( data.reverse() );
            }else{
                this.fillData( data );
            }
        },  

    /* Fill in the table with the value getting from the server */
    fillData: function( value ) {
        var _html = "<table width='100%'> <th class='ui-widget-header ui-big-pad'>";
        if( this.options.selectable ){ 
        _html += "<input type='checkbox' onClick='checkboxHandler( $(this) );'> </input>";
        }
        _html += "</th>";
        for( v in this.options.header ){
            _html += "<th class='ui-widget-header ui-big-pad' style='text-transform:uppercase;height:20px;' title='"+ this.options.headerinfo[v] +"'>" + this.options.header[v] + "</th>";
        }
        for( v in this.options.data ){
            _html += "<tr>";
            for( h in this.options.header ){
                if(h == 0){
                    _html += "<td align='center' class='ui-big-pad' colspan=2>" + this.options.data[v][h] + "</td>";
                }else{
                    _html += "<td align='center' class='ui-big-pad' >" + this.options.data[v][h] + "</td>";
                }
            }
            _html += "</tr>";
        }
        _html += "</table>";
        //add the table element to the current div item so it can be filled with the data table.
        this.element.html( _html );
        var tables = this.element.find( "table" );
        if(this.options.selectable){
            tables.uitable();
        }
        //configure "th" to trigger a "headerclicked" event when some element in the header is clicked.
        var ths = this.element.find( "th:gt(0)" );
        ths.each( function(){
            $(this).click( function(){
               //We want to trigger a "headerclicked" event whent the header is clicked.
                $(this).trigger( "headerclicked", $(this).html() );
            }).mouseenter( function(){
                $(this).css( "cursor", "pointer" );
            }).mouseleave( function(){
                $(this).css( "cursor", "auto" );
            });
        });
    },
   
    /* Fills the selected_row_keys parameter (Array) with */
    getSelected: function( selected_row_keys ){
        //console.log("getting selected");
        var rows = this.element.find( "tr:gt(0)" ); //the first row is a header so we ignore it.
        var key = this.options.keyindex;
        rows.each( function() {
            if( $( this ).hasClass( "ui-state-active" ) ){
                var _cols = $(this).children();
                selected_row_keys.push( _cols[key].innerHTML );
            }
        });         
    },
    
    /* Function that returns the number of rows of data in this data table. */
    getNumRows: function( num_rows ){
    	var rows = this.element.find( "tr:gt(0)" );
      //console.log( rows.length );
    	num_rows = rows.length;
      return num_rows;
    },

	destroy: function() {			
  		this.element.html( "" );
	  },

    _setOptions: function(){
        $.Widget.prototype._setOptions.apply(this, arguments);
    },

	_setOption: function( option, value ) {
		$.Widget.prototype._setOption.apply( this, arguments );
		
		switch ( option ) {
            case "selectall":
                this.element.find( "input" ).attr( "checked", value);
                checkboxHandler( this.element.find( "input" ).first(), null );
                break;
            case "selectfirst":
                $($(this.element.find("table").first()).find("tr")[1]).addClass( "ui-state-active" );
            	break;
            case "selectsome":
            	var rows = $(this.element.find("table").first()).find("tr");
            	for(var i = 0; i < value.length; i++){
            	    if (value[i]){
            	        $(rows[i+1]).addClass( "ui-state-active" );
            	    }else{
            	        $(rows[i+1]).removeClass( "ui-state-active" );
            	    }
            	}
            	break;
            case "width":
                this.element.css( "width", value );
                break;
            case "height":
                this.element.css( "height", value );
                break;
            case "data":
                this.fillData( value );
                break;
            case "getselected":
                this.getSelected( value );
                break;
	    }
	}
});
})( jQuery );
