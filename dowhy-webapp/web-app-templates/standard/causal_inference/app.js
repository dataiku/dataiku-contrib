function el(id) {
    return document.getElementById(id);
}

$(document).ready(function () {
    $.getJSON(getWebAppBackendUrl('datasets'), function (data) {
        let newSelectDataset = el('chooseDataset');
        let selectDatasetHTML = '<option disabled selected value> -- select a dataset -- </option>';
        for (let datasetName of data.dataset_names) {
            selectDatasetHTML += `<option value="${datasetName}">${datasetName}</option>`;
        };
        newSelectDataset.innerHTML = selectDatasetHTML;
    });
    el('chooseDataset').addEventListener('input', selectColumn);
    el('add').addEventListener('click', addDot);
    el('remove').addEventListener('click', removeDot);
    el('register').addEventListener('click', registerGraph);
    el('visualize').addEventListener('click', visualizeGraph);
    el('error-close').addEventListener('click', hideError);
});

function selectColumn() {
    $('.causality-settings, .causality-output').show();
    let dataset = el('chooseDataset').value;
    let params = {
        dataset: dataset
    };
    $.getJSON(getWebAppBackendUrl('columns'), params, function (data) {
        autocomplete(el('nodeLeft'), data.columns);
        autocomplete(el('nodeRight'), data.columns);
        autocomplete(el('treatmentName'), data.columns);
        autocomplete(el('outcomeName'), data.columns);
    });
}

function emptyInput(node1, node2) {
    node1.value = '';
    node2.value = '';
}

function addDot() {
    let nodeLeft = el('nodeLeft');
    let nodeRight = el('nodeRight');
    let dotField = el('dotField');
    let nodeLeftVal = nodeLeft.value;
    let nodeRightVal = nodeRight.value;

    if (nodeLeftVal && nodeRightVal) {
        dotField.value += `"${nodeLeftVal}" -> "${nodeRightVal}"; `;
        emptyInput(nodeLeft, nodeRight);
        el('graph').innerHTML = '';
        dotField = el('dotField');
        let digraph = 'digraph {' + dotField.value + '}'
        d3.select("#graph")
            .graphviz()
            .renderDot(digraph);
    } else {
        setError('missing node(s)')
    }
}

function removeDot() {
    let nodeLeft = el('nodeLeft');
    let nodeRight = el('nodeRight');
    let dotField = el('dotField');
    let nodeLeftVal = nodeLeft.value;
    let nodeRightVal = nodeRight.value;

    if (nodeLeftVal && nodeRightVal) {
        let pattern = `"${nodeLeftVal}" -> "${nodeRightVal}"; `;
        dotField.value = dotField.value.replace(pattern, '')
        emptyInput(nodeLeft, nodeRight);
        el('graph').innerHTML = '';
        dotField = el('dotField');
        let digraph = 'digraph {' + dotField.value + '}';
        d3.select("#graph")
            .graphviz()
            .renderDot(digraph);
    } else {
        setError('missing node(s)')
    }
}

function visualizeGraph() {
    el('graph').innerHTML = '';
    let dotField = el('dotField');
    let digraph = `digraph {${dotField.value}}`;
    d3.select("#graph")
        .graphviz()
        .renderDot(digraph);
};

function setError(message) {
    el('error-content').innerHTML = message;
    $('#error-block').show();
}

function hideError() {
    $('#error-block').hide();
}

function registerGraph() {
    el('graph').innerHTML = '';
    el('results').innerHTML = '';
    let dotField = el('dotField');
    let chooseDataset = el('chooseDataset');
    let outcomeName = el('outcomeName');
    let treatmentName = el('treatmentName');
    let dataset = chooseDataset.value;
    let digraph = 'digraph {' + dotField.value + '}';
    let outcome = outcomeName.value;
    let treatment = treatmentName.value;
    if (outcome && treatment && dataset && digraph) {
        d3.select("#graph")
            .graphviz()
            .renderDot(digraph);
        params = {
            'digraph': digraph,
            'dataset': dataset,
            'outcome': outcome,
            'treatment': treatment
        }
        $.getJSON(getWebAppBackendUrl('register-graph'), params, function (d) {
            let results = d.results;
            el('results').innerHTML = results;
        })
        .error(() => setError('Failed to register graph, check the logs'));
    } else {
        setError('Missing one of the following: Treatment, Outcome, Dataset or Digraph')
    }
}

function autocomplete(c,d){function f(k){return!!k&&void(g(k),j>=k.length&&(j=0),0>j&&(j=k.length-1),k[j].classList.add("autocomplete-active"))}function g(k){for(let l=0;l<k.length;l++)k[l].classList.remove("autocomplete-active")}function h(k){let l=document.getElementsByClassName("autocomplete-items");for(let m=0;m<l.length;m++)k!=l[m]&&k!=c&&l[m].parentNode.removeChild(l[m])}let j;c.addEventListener("input",function(){let l=this.value;if(h(),!l)return!1;j=-1;let m=document.createElement("DIV");m.setAttribute("id",this.id+"autocomplete-list"),m.setAttribute("class","autocomplete-items"),this.parentNode.appendChild(m);for(let n=0;n<d.length;n++)if(d[n].substr(0,l.length).toUpperCase()==l.toUpperCase()){let o=document.createElement("DIV");o.innerHTML="<strong>"+d[n].substr(0,l.length)+"</strong>",o.innerHTML+=d[n].substr(l.length),o.innerHTML+="<input type='hidden' value='"+d[n]+"'>",o.addEventListener("click",function(){c.value=this.getElementsByTagName("input")[0].value,h()}),m.appendChild(o)}}),c.addEventListener("keydown",function(k){let l=el(this.id+"autocomplete-list");l&&(l=l.getElementsByTagName("div")),40==k.keyCode?(j++,f(l)):38==k.keyCode?(j--,f(l)):13==k.keyCode&&(k.preventDefault(),-1<j&&l&&l[j].click())}),document.addEventListener("click",function(k){h(k.target)})}
