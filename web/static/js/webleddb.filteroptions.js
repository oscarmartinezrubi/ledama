(function($) {
    $.widget("webleddb.filteroptions",{
		options: {
            updateColNames: null,
            getQueryObjects: null,
            reset: null
		},
				
		_create: function() {
            var relOps = [
                "=",
                "!=",
                ">",
                "<",
                "~",
                "!~"
            ];

            var tempHeader = [
            "LDS",
            "LDSB",
            "Afad",
            "adsfa"
            ];
            
            var autoHandler = function(){
                    $( this ).
                        css("color", "rgb(0, 0, 0)").
                        val("").
                        autocomplete( "search", "" );
            };
            //specify the greyed out color for initial values shown.
            this.faintColor = "rgb(180, 180, 180)"; 
            this.element.addClass( "ui-widget-content" );

            this.colname = $( "<input></input>" ).
                css({
                "margin": "2px",
                "width": "90px",
                    "height": "25px",
                "color": this.faintColor
                }).
                focus(autoHandler).
                val( "ColName" ).
                autocomplete({source:tempHeader, minLength:0});

            this.relation = $( "<input></input>" ).
                val( "rel" ).
                css({
                    "margin": "2px",
                    "width": "30px",
                    "height": "25px",
                    "color":  this.faintColor
                    }).
                focus(autoHandler).
               autocomplete({source:relOps, minLength:0});

            this.values = $( "<input ></input>" ).
                val( "values" ).
                css({
                    "margin": "2px",
                    "width": "100px",
                    "height": "25px",
                    "color":  this.faintColor
                    });

            //using js closure to ensure that the add button knows what data to add.
            var box = this.element;
            var colname = this.colname;
            var relation = this.relation;
            var values = this.values;
            this.addButton = $( "<input type='button' value='add'></input>" ).
                click(function(){
                    //var row = $('<input type="text" value="' + colname.val() + " " + relation.val() + " " + values.val() + '"></input>'); 
                    var row = $('<div></div>'); 
                    var c = $('<input type="text" name="key" value="' + colname.val() + '"></input>').css("width","90px");
                    var o = $('<input type="text" name="op" value="' + relation.val() + '"></input>').css("width","20px");
                    var v = $('<input type="text" name="val" value="' + values.val() + '"></input>').css("width","100px");
                    row.append(c, o, v);
                    row.find('input').css({
                        "border" : "0px",
                        "text-align" : "center"
                    });

                    var rem = $("<button>Rem</button>");
                    row.
                    addClass("filterops").
                    css({
                        "border" : "0px",
                        "padding": "0px",
                        "width": "300px"
                    });

                    rem.
                    addClass("filterops_rem").
                    button({
                        icons:{
                            primary: "ui-icon-trash",
                        },
                        text: false
                    }).
                    click(function(){
                        //remove this row and the button when rem is clicked.
                        row.remove();
                        rem.remove();
                    });
                    row.append(rem);
                    row.insertAfter( $(this) );
                }).
                button({
                });
            this.element.css({
                "padding" : "5px",
                "width" : "285px"
            }).
            append( this.colname, this.relation, this.values, this.addButton );
       	},
	
        /* Function to create an array of objects that represent the query selected
        by the user */
        getQueryObjects: function(){
            var arr = new Array();
            var rows = this.element.find(".filterops").each(function(){
                var obj = {};
                obj.key = $(this).find('input[name="key"]').val();
                obj.op =  $(this).find('input[name="op"]').val();
                obj.val =  $(this).find('input[name="val"]').val();
                arr.push(obj);
            });
            return arr;
        },

        /* Function to update the colum names that are shown when user clicks on it. */
        updateColNames: function(list){
            this.colname.autocomplete("option", "source", list);
        },
        
        /* remove the queries added and set the col,rel and vals to default text */
        reset: function(){
            //reset the colname, rel and values textboxes 
            this.colname.
                val( "ColName" ).
                css( "color", this.faintColor );
            this.relation.
                val( "rel" ).
                css( "color", this.faintColor );
            this.values.
                val( "Values" ).
                css( "color", this.faintColor );
            //get rid of the rows of text.
            var rows = this.element.find(".filterops").each(function(){
                $(this).remove();
            });
            //get rid of rem the buttons.
            rows = this.element.find(".filterops_rem").each(function(){
                $(this).remove();
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
