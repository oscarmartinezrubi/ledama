(function($) {
    $.widget("webleddb.storagenodestat", {
		options: {
            switchView: null,
            'nodeid': 'Node 007',
            'disks' : []
		},
				
		_create: function(){
            this.nodeid = this.options.nodeid;
            this.disks = this.options.disks;
            
            //label for the node.
            this.ui_title = $( "<div></div>" ).
                    addClass( 'storagenodestat-title' ).
                    append( this.nodeid ); //assuming node id is of "nodexxx" form.
            if( this.disks.length == 0 ){
                this.ui_title.addClass( 'ui-state-error' );
            }else{
                this.ui_title.addClass( 'ui-state-active' );
            }

            /*
              jqueryui's progress bar doesn't provide a label option to show on the progressbar. 
              We have to creat it ourselves. We use the "nodestat-progressbar-label" and "nodestat-progressbar"
              defined in the "clustermonitor.css"  for setting the width and height.
            */
            //add the storage info
            this.ui_disks = $( "<div></div>" ).
                addClass( 'nodestat-disks' ).
                append( '<b>Disk:</b>' );
            var totalFree = 0;
            //console.log( this.nodeid + ":" + this.disks );
            for( var i = 0; i < this.disks.length; i++ ){
                var disk_info = this.disks[i];
                var label = $( "<div></div>" ).
                    addClass( 'nodestat-progressbar-label' );
                var disk = $( "<div></div>" ).
                    addClass( 'nodestat-progressbar' );
                if( disk_info[0] < 0 ){
                    disk.addClass( 'ui-state-error ui-corner-all' );
                    label.html( "NA" ).css({
                            "text-align": "center",
                            "font-weight": "bold"
                            }).removeClass('nodestat-progressbar-label');
                }else{
                    totalFree += parseInt( disk_info[1] );
                    disk.progressbar({
                        value: disk_info[0]
                    });
                    label.html( disk_info[0] + "% " + disk_info[1] );
                    var color_class = "nodestat-disk-green";
                    if( disk_info[0] > 90){
                       color_class = "nodestat-disk-red"; 
                    }else if( disk_info[0] > 80 ){
                        color_class = "nodestat-disk-blue";
                    }
                    disk.find('.ui-progressbar-value').
                        removeClass( 'ui-widget-header' ).
                        addClass( color_class );
                }
                disk.append( label );
                this.ui_disks.append( disk );
            }

            //add the new info to the element.
            this.element.
                addClass( 'storagenodestat  ui-state-highlight' ).
                append( this.ui_title, this.ui_disks);
            //we hide the disks ui and the net ui because we set "cpu" as the default in the clustermonitor.js 
            this.element.hide();
        },
        
        switchView: function( obj ){
            if (obj._show == "disk"){
                $(this.element).show('slide', 2000);            
            }
           if (obj._hide == "disk"){
                $(this.element).hide('slide', 2000);
            }
        },

        _setOptions: function(){
            $.Widget.prototype._setOptions.apply(this, arguments);
        },

		_setOption: function( option, value ) {
			$.Widget.prototype._setOption.apply( this, arguments );
            switch( option ){
               case "nodeid":
                    this.nodeid = value;
                    break;
                case "disks":
                    this.disks = value;
                    var disks = this.ui_disks.find( ".nodestat-progressbar" );
                    var disk_labels = this.ui_disks.find( ".nodestat-progressbar-label" );
                    for( var i = 0; i < value.length; i++ ){
                       var disk_info = value[i];
                       if( disk_info[0] > -1 ){
                           $( disks[i] ).progressbar( { "value" : disk_info[0] } );
                           $( disk_labels[i] ).html( disk_info[0] + "% " + disk_info[1] );                       
                       }
                    }
                    break;
            }
        }
	});
})( jQuery );
