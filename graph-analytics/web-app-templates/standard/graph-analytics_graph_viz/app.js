var newSelectDataset = document.createElement('select');
newSelectDataset.className = "form-control";
newSelectDataset.id = "chooseDataset";
    
var newSelectColumnSource = document.createElement('select');
newSelectColumnSource.className = "form-control";
newSelectColumnSource.id = "chooseColumnSource";

var newSelectColumnTarget = document.createElement('select');
newSelectColumnTarget.className = "form-control";
newSelectColumnTarget.id = "chooseColumnTarget";

var newSelectColumnCut = document.createElement('select');
newSelectColumnCut.className = "form-control";
newSelectColumnCut.id = "chooseColumnCut";

$(document).ready(function(){
    
   document.getElementById('chooseDatasetDiv').appendChild(newSelectDataset);
   document.getElementById('chooseColumnDivSource').appendChild(newSelectColumnSource);
   document.getElementById('chooseColumnDivTarget').appendChild(newSelectColumnTarget);
   document.getElementById('chooseColumnDivCut').appendChild(newSelectColumnCut);
   
    $.getJSON(getWebAppBackendUrl('datasets'),function(data){
        
        var selectDatasetHTML = "";
        var dataset_names =  data.dataset_names,
            datasetLen = dataset_names.length;
        
        for(i = 0; i < datasetLen; i++){
            selectDatasetHTML += "<option value='" + dataset_names[i] + "'>" + dataset_names[i]+"</option>";
        };

        newSelectDataset.innerHTML = "<option value='Dataset'>Dataset</option>" + selectDatasetHTML;
        document.getElementById("chooseDataset").addEventListener("input", x => selectColumn(newSelectDataset.value));
      
        
 
    });
        
});

function selectColumn(datasetName){
    
    var params = {
        "dataset": datasetName
    };
    
    $.getJSON(getWebAppBackendUrl('columns'), params , function(d) {
        
        var selectColumnHTML = "";
        var columns = d.columns,
            columnsLen = columns.length;
        
        for(i = 0; i < columnsLen; i++){

                selectColumnHTML += "<option value='" + columns[i] + "'>" + columns[i]+"</option>";
            }; 
    
    newSelectColumnSource.innerHTML=selectColumnHTML;
    newSelectColumnTarget.innerHTML=selectColumnHTML;
    newSelectColumnCut.innerHTML=selectColumnHTML;
});
};


$("#btn").on("click", function() {
    
    var similarity = document.getElementById("similarityInput").value;
    var node_source = newSelectColumnSource.value;
    var node_target = newSelectColumnTarget.value;
    var interactions = newSelectColumnCut.value;
    var dataset = newSelectDataset.value;
    
$("#graph-container").empty();
    
$.getJSON(getWebAppBackendUrl('draw_graph'), 
          {similarity: similarity,
           node_source: node_source,
           node_target: node_target,
           interactions: interactions,
           dataset: dataset}, 
           function(data) {

  var width = 900;
  var height = 900;
  var colorScale = d3.scale.category20();
  var sizeExtent = d3.extent(data.graph.nodes, function(d) {
    return d.degree;
  });
  console.log('sizeExtent: ' + sizeExtent);
  var sizeScale = d3.scale.sqrt()
    .domain(sizeExtent)
    .range([5, 10]);

  // customize color and tooltip
  function colorFunc(d) {
    return colorScale(parseInt(d.community));
  }

  function tooltipFunc(d) {
    return d.id + '<br>' + 'degree: ' + d.degree;
  }

  function sizeFunc(d) {
    return sizeScale(d.degree);
  }

  // d3 tooltip
  var tip = d3.tip()
    .attr('class', 'd3-tip')
    .direction('n')
    .offset([-10, 0])
    .html(tooltipFunc);

  // d3 force layout
  var force = d3.layout.force()
    .linkDistance(25)
    .charge(-50)
    .size([width, height]);

  var svg = d3.select("#graph-container").select("svg");

  if (svg.empty()) {
    svg = d3.select("#graph-container").append("svg")
      .attr("width", width)
      .attr("height", height);
    svg.call(tip);
  };

  force.nodes(data.graph.nodes)
    .links(data.graph.links)
    .start();

  var link = svg.selectAll(".link")
    .data(force.links())
    .enter()
    .append("path")
    .attr("class", "link");

  var node = svg.selectAll(".node")
    .data(force.nodes())
    .enter()
    .append("circle")
    .attr("class", "node")
    .attr("r", sizeFunc)
    .style("fill", colorFunc)
    .call(force.drag);

  force.on("tick", function() {

    link.attr("d", function(d) {
      var dx = d.target.x - d.source.x,
        dy = d.target.y - d.source.y,
        dr = Math.sqrt(dx * dx + dy * dy);
      return "M" + d.source.x + "," + d.source.y + "A" + dr + "," + dr + " 0 0,1 " + d.target.x + "," + d.target.y;
    });

    node.attr("transform", function(d) {
      return "translate(" + d.x + "," + d.y + ")";
    });

  });

  // d3 tooltip on node
  node.on("mouseover", tip.show)
    .on("mouseout", tip.hide);
  // node highlight
  node.on('dblclick', connectedNodes);

  //Toggle stores whether the highlighting is on
  var toggle = 0;
  //Create an array logging what is connected to what
  var linkedByIndex = {};
  for (i = 0; i < data.graph.nodes.length; i++) {
    linkedByIndex[i + "," + i] = 1;
  };
  data.graph.links.forEach(function(d) {
    linkedByIndex[d.source.index + "," + d.target.index] = 1;
  });
  //This function looks up whether a pair are neighbours
  function neighboring(a, b) {
    return linkedByIndex[a.index + "," + b.index];
  }

  function connectedNodes() {
    if (toggle == 0) {
      //Reduce the opacity of all but the neighbouring nodes
      d = d3.select(this).node().__data__;
      node.style("opacity", function(o) {
        return neighboring(d, o) | neighboring(o, d) ? 1 : 0.1;
      });
      link.style("opacity", function(o) {
        return d.index == o.source.index | d.index == o.target.index ? 1 : 0.1;
      });
      //Reduce the op
      toggle = 1;
    } else {
      //Put them back to opacity=1
      node.style("opacity", 1);
      link.style("opacity", 1);
      toggle = 0;
    }
  }

});
    
});
