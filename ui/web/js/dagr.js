// ideas:
// - font awesome
// - jquery-ui date picker
// - jquery-ui controlgroup

var taskDataTable = null;
var detailsControlClassPlus = "glyphicon-plus-sign";
var detailsControlClassMinus = "glyphicon-minus-sign";

// Add event listener for opening and closing details
function taskTableEventListener() {
    $('#tasks tbody').on('click', 'td.details-control', function () {
        var tr = $(this).closest('tr');
        var row = taskDataTable.row( tr );
 
		var firstColumn = tr.find('td:eq(0)');
		if ( firstColumn.hasClass(detailsControlClassMinus) ) {
            // This row is already open - close it
            row.child.hide();
			firstColumn.removeClass( detailsControlClassMinus );
			firstColumn.addClass( detailsControlClassPlus );
            tr.removeClass('shown');
        }
        else {
			firstColumn.removeClass( detailsControlClassPlus );
			firstColumn.addClass( detailsControlClassMinus );
            // Open this row
			var id = 'id-' + tr.find('td:eq(2)').text() + '-tabs'; 
            row.child( format(id, row.data()) ).show();
            tr.addClass('shown');
			//$( '#'+id ).tabs();
        }
    } );
}

/* Formatting function for row details - modify as you need */
function format (id,  d ) {
	// `id` is the id of the task (should be unique)
    // `d` is the original data object for the row
	return '<div id="'+id+'">'+
		'<ul class="nav nav-tabs">'+
			'<li class="active"><a data-toggle="tab" href="#'+id+'-li-1">Script</a></li>'+
			'<li><a data-toggle="tab" href="#'+id+'-li-2">Log</a></li>'+
			'<li><a data-toggle="tab" href="#'+id+'-li-3">Depends On</a></li>'+
			'<li><a data-toggle="tab" href="#'+id+'-li-4">Dependents</a></li>'+
			'<li><a data-toggle="tab" href="#'+id+'-li-5">Parent</a></li>'+
			'<li><a data-toggle="tab" href="#'+id+'-li-6">Children</a></li>'+
		'</ul>'+
		'<div id='+id+'-content" class="tab-content">'+
			'<div id="'+id+'-li-1" class="tab-pane fade in active"><p>'+d.script+'</p></div>'+
			'<div id="'+id+'-li-2" class="tab-pane fade"><p>'+d.log+'</p></div>'+
			'<div id="'+id+'-li-3" class="tab-pane fade"><p>'+d.depends_on+'</p></div>'+
			'<div id="'+id+'-li-4" class="tab-pane fade"><p>'+d.dependents+'</p></div>'+
			'<div id="'+id+'-li-5" class="tab-pane fade"><p>'+d.parent+'</p></div>'+
			'<div id="'+id+'-li-6" class="tab-pane fade"><p>'+d.children+'</p></div>'+
		'</div>'+
	'</div>';
}


function initOrUpdateTaskDataTable() {
	if (taskDataTable == null) {
		taskDataTable = $('#tasks').DataTable( {
			"columns": [
				{
 					"className":      'details-control glyphicon '+detailsControlClassPlus,
                	"orderable":      false,
                	"data":           null,
                	"defaultContent": ''
				},
				{ "data": "name" },
				{ "data": "id" },
				{ "data": "status" },
				{ "data": "description" },
				{ "data": "attempts" },
				{ "data": "last" },
				{ "data": "script" },
				{ "data": "log" },
				{ "data": "depends_on" },
				{ "data": "dependents" },
				{ "data": "parent" },
				{ "data": "children" }
			],
			"destroy": true,
			"order": [[2, 'asc']]
		} );
		taskDataTable	
			.columns( '.glyphicon' )
			.header()
			.to$()
			.removeClass( 'glyphicon' )
			.removeClass( detailsControlClassPlus )
		taskTableEventListener();
	}
	else {
		taskDataTable.rows().invalidate().draw();
	}
}
