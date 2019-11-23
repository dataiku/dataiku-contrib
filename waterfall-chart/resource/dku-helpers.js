/*
Helper function to query webapp backend with a default implementation for error handling
v 1.0.1
*/

const webappBackend = (function() {
    function getUrl(path) {
        return dataiku.getWebAppBackendUrl(path);
    }

    function dkuDisplayError(error) {
        alert('Backend error, check the logs.');
    }

    function get(path, args={}, displayErrors=true) {
        return fetch(getUrl(path) + '?' + $.param(args), {
            method: 'GET',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
        })
        .then(response => {
            if (response.status == 502) {
                throw Error("Webapp backend not started");
            } else if (!response.ok) {
                throw Error(`${response.statusText} (HTTP ${response.status})`);
            }
            try {
                return response.json();
            } catch {
                throw Error('The backend response is not JSON: '+ response.text());
            }
        })
        .catch(function(error) {
            if (displayErrors && error.message && !error.message.includes('not started')) { // little hack, backend not started should be handled elsewhere
                dkuDisplayError(error);
            }
            throw error;
        });
    }

    return Object.freeze({getUrl, get});
})();


const webappMessages = (function() {
    function displayFatalError(err) {
        const errElt = $('<div class="fatal-error" style="margin: 100px auto; text-align: center; color: var(--error-red)"></div>')
        errElt.text(err);
        $('#dku_html').html(errElt);
    }
    
    return Object.freeze({displayFatalError})
})();