(function($) {
    $.widget("webleddb.nodestat", {
		options: {
            switchView: null,
            'nodeid': 'Node 007',
            'cpu': '0',
            'mem': '0',
            'net': [],
            'disks' : []
		},
				
		_create: function(){
            this.nodeid = this.options.nodeid;
            this.cpu = Math.round(this.options.cpu/1600 * 100);
            this.mem = Math.round(this.options.mem);
            this.disks = this.options.disks;
            this.net = this.options.net;
            
            //label for the node.
            this.ui_title = $( "<div></div>" ).
                    addClass( 'nodestat-title' ).
                    append( this.nodeid ); //assuming node id is of "nodexxx" form.
            if( this.disks.length == 0 || this.cpu[0] < 0 || this.mem < 0 ){
                this.ui_title.addClass( 'ui-state-error' );
            }else{
                this.ui_title.addClass( 'ui-state-active' );
            }

            /*
              jqueryui's progress bar doesn't provide a label option to show on the progressbar. 
              We have to creat it ourselves. We use the "nodestat-progressbar-label" and "nodestat-progressbar"
              defined in the "clustermonitor.css"  for setting the width and height.
            */
            //memory info of the system
            this.ui_mem_label = $( "<div></div>" ).addClass( 'nodestat-progressbar-label' );
            this.ui_mem = $( "<div></div>" ).
                        addClass( 'nodestat-progressbar' ).
                        progressbar({
                            value:this.mem
                        }).append( this.ui_mem_label );
            this.ui_mem_label.html( this.ui_mem.progressbar( "option", "value" ) + "%" );
            if(this.mem == 0){
                this.ui_mem_label.css("top", "0");
            }

            //set up the ui for cpuinfo
            this.ui_cpu_label = $( "<div></div>" ).addClass( 'nodestat-progressbar-label' );
            this.ui_cpu = $( "<div></div>" ).
                addClass( 'nodestat-progressbar' ).
                progressbar({
                        max: 100,
                        value:this.cpu
                }).append( this.ui_cpu_label );
            this.ui_cpu_label.html( this.ui_cpu.progressbar( "option", "value" ) + "%" );
            if(this.cpu == 0){
                this.ui_cpu_label.css("top", "0");
            }
            this.ui_usageinfo = $( "<table></table>" ).
                append( "<tr><td><b>CPU:</b></td><td id='infocpu'> </td></tr>" ). 
                append( "<tr><td><b>Mem:</b></td><td id='infomem'> </td></tr>" ); 
            this.ui_usageinfo.find( "td[id='infocpu']" ).append( this.ui_cpu );
            this.ui_usageinfo.find( "td[id='infomem']" ).append( this.ui_mem );
            this.ui_usageinfo.addClass( 'nodestat-cpus' );

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

            this.ui_netinfo = $( "<div></div>" ).
                addClass( 'nodestat-net' ).
                append( "<b> Eth0 </b>" );
            this.dl_info = $( "<div></div>" ).addClass( 'nodestat-progressbar' );
            this.dl_label = $( "<div></div>" ).addClass( 'nodestat-progressbar-label' );
            this.ul_info = $( "<div></div>" ).addClass( 'nodestat-progressbar' );
            this.ul_label = $( "<div></div>" ).addClass( 'nodestat-progressbar-label' );
            this._updateNet();
            this.ui_netinfo.append( this.dl_info, this.ul_info );

            //add the new info to the element.
            this.element.
                addClass( 'nodestat  ui-state-highlight' ).
                append( this.ui_title, this.ui_disks, this.ui_usageinfo, this.ui_netinfo );
            //we hide the disks ui and the net ui because we set "cpu" as the default in the clustermonitor.js 
            this.ui_disks.hide();
            this.ui_netinfo.hide();

            //triger a nodeclicked event when cpu info is clicked.
            var nodeid = this.nodeid;
            //configure the disk info to trigger node clicked event when mouse pressed. 
            this.ui_disks.
                mouseenter(function(){
                    $(this).css( "cursor", "pointer" );
                }).mouseleave(function(){
                    $(this).css( "cursor", "auto" );
                }).click(function(){
                    $(this).trigger( "nodeclicked", {"id":nodeid, "type":"disk"} );
                });
            //configure the usage info to trigger node clicked event when mouse pressed. 
            this.ui_usageinfo.
                mouseenter(function(){
                    $(this).css( "cursor", "pointer" );
                }).mouseleave(function(){
                    $(this).css( "cursor", "auto" );
                }).click(function(){
                    $(this).trigger( "nodeclicked", {"id":nodeid, "type":"cpu"} );
                });
        },
        
        switchView: function( obj ){
            var obj_to_hide = null;
            var obj_to_show = null;
            switch( obj._hide ){
                case "cpu":
                    obj_to_hide = this.ui_usageinfo;
                    break;
                case "disk":
                    obj_to_hide = this.ui_disks;
                    break;
                case "net":
                    obj_to_hide = this.ui_netinfo;
                    break;
            }
            switch( obj._show ){
                case "cpu":
                    obj_to_show = this.ui_usageinfo;
                    break;
                case "disk":
                    obj_to_show = this.ui_disks;
                    break;
                case "net":
                    obj_to_show = this.ui_netinfo;
                    break;
            }
            obj_to_hide.hide( 'slide', 1000, function(){
                obj_to_show.show( 'slide', 1000 );
            });
        },
        
        _updateNet: function(){
            //Update the network ui based on the values set in the "this.net" variable.
            var download = Math.round(this.net[0][1]/(1048576)); //convert from B to MB.
            var upload = Math.round(this.net[0][2]/(1048576)); 

            this.dl_info.progressbar({
                value: download,
                max: 128// MB 
            }).append( this.dl_label );

            this.ul_info.progressbar({
                value: upload,
                max: 128 //MB
            }).append( this.ul_label );

            var dl_str = "<p><span style='float:left;' class='ui-icon ui-icon-circle-arrow-s'></span>" + download + " MB/s</p>";
            var ul_str = "<p><span style='float:left;' class='ui-icon ui-icon-circle-arrow-n'></span>" + upload + " MB/s</p>";

            //replace the labels with the updated strings.
            this.dl_label.html( dl_str );
            this.ul_label.html( ul_str );

            /*
                when the progressbar shows up, the label that we use goes below the progressbar. 
                We have to move it up so it is still visible.
            */
            if(download > 0){
                this.dl_label.css("top", "-22px");
            }else{
                this.dl_label.css("top", "-11px");
            }

            if(upload > 0){
                this.ul_label.css("top", "-22px");
            }else{
                this.ul_label.css("top", "-11px");
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
                case "mem":
                    this.mem = Math.round( value );
                    this.ui_mem.progressbar( "option", "value", this.mem );
                    this.ui_mem_label.html( this.mem + "%" );
                    if(this.mem > 0){
                        this.ui_mem_label.css("top", "-12px");
                    }else{
                        this.ui_mem_label.css("top", "0px");
                    }
                    break;
                case "cpu":
                    this.cpu = Math.round( value/1600 * 100 );
                    this.ui_cpu.progressbar( "option", "value", this.cpu );
                    this.ui_cpu_label.html( this.cpu + "%" );
                    if(this.cpu > 0){
                        this.ui_cpu_label.css("top", "-12px");
                    }else{
                        this.ui_cpu_label.css("top", "0px");
                    }
                    break;
                case "net":
	               this.net = value;
                   this._updateNet();
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
