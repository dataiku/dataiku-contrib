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

function autocomplete(inp, arr) {
    /* the autocomplete function takes two arguments, the text field element and an array of possible autocompleted values: */
    let currentFocus;
    /*execute a function when someone writes in the text field:*/
    inp.addEventListener("input", function (e) {
        let val = this.value;
        /*close any already open lists of autocompleted values*/
        closeAllLists();
        if (!val) {
            return false;
        }
        currentFocus = -1;
        /*create a DIV element that will contain the items (values):*/
        let a = document.createElement("DIV");
        a.setAttribute("id", this.id + "autocomplete-list");
        a.setAttribute("class", "autocomplete-items");
        /*append the DIV element as a child of the autocomplete container:*/
        this.parentNode.appendChild(a);
        /*for each item in the array...*/
        for (let i = 0; i < arr.length; i++) {
            /*check if the item starts with the same letters as the text field value:*/
            if (arr[i].substr(0, val.length).toUpperCase() == val.toUpperCase()) {
                /*create a DIV element for each matching element:*/
                let b = document.createElement("DIV");
                /*make the matching letters bold:*/
                b.innerHTML = "<strong>" + arr[i].substr(0, val.length) + "</strong>";
                b.innerHTML += arr[i].substr(val.length);
                /*insert a input field that will hold the current array item's value:*/
                b.innerHTML += "<input type='hidden' value='" + arr[i] + "'>";
                /*execute a function when someone clicks on the item value (DIV element):*/
                b.addEventListener("click", function (e) {
                    /*insert the value for the autocomplete text field:*/
                    inp.value = this.getElementsByTagName("input")[0].value;
                    /*close the list of autocompleted values,
                    (or any other open lists of autocompleted values:*/
                    closeAllLists();
                });
                a.appendChild(b);
            }
        }
    });

    /*execute a function presses a key on the keyboard:*/
    inp.addEventListener("keydown", function (e) {
        let x = el(this.id + "autocomplete-list");
        if (x) x = x.getElementsByTagName("div");
        if (e.keyCode == 40) {
            /*If the arrow DOWN key is pressed,
            increase the currentFocus variable:*/
            currentFocus++;
            /*and and make the current item more visible:*/
            addActive(x);
        } else if (e.keyCode == 38) { //up
            /*If the arrow UP key is pressed,
            decrease the currentFocus variable:*/
            currentFocus--;
            /*and and make the current item more visible:*/
            addActive(x);
        } else if (e.keyCode == 13) {
            /*If the ENTER key is pressed, prevent the form from being submitted,*/
            e.preventDefault();
            if (currentFocus > -1) {
                /*and simulate a click on the "active" item:*/
                if (x) x[currentFocus].click();
            }
        }
    });

    function addActive(x) {
        /*a function to classify an item as "active":*/
        if (!x) return false;
        /*start by removing the "active" class on all items:*/
        removeActive(x);
        if (currentFocus >= x.length) currentFocus = 0;
        if (currentFocus < 0) currentFocus = (x.length - 1);
        /*add class "autocomplete-active":*/
        x[currentFocus].classList.add("autocomplete-active");
    }

    function removeActive(x) {
        /*a function to remove the "active" class from all autocomplete items:*/
        for (let i = 0; i < x.length; i++) {
            x[i].classList.remove("autocomplete-active");
        }
    }

    function closeAllLists(elmnt) {
        /*close all autocomplete lists in the document,
        except the one passed as an argument:*/
        let x = document.getElementsByClassName("autocomplete-items");
        for (let i = 0; i < x.length; i++) {
            if (elmnt != x[i] && elmnt != inp) {
                x[i].parentNode.removeChild(x[i]);
            }
        }
    }
    /*execute a function when someone clicks in the document:*/
    document.addEventListener("click", function (e) {
        closeAllLists(e.target);
    });
}