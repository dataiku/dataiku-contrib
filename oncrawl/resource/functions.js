function prettyPrint(oql) {
    
    try
    {
        var pretty_oql = JSON.parse(oql);
    }
    catch(e)
    {
        var pretty_oql = oql
    }
    
    pretty_oql = JSON.stringify(pretty_oql, undefined, 4);
    
    return pretty_oql;
}

function build_oql(scope, reset)
{
    let oql_templates = {
        'aggs' : {
            'pages' : '{"aggs":[{"oql":{"field":["fetched","equals","true"]},"name":"agg_name"}]}',
            'links' : '{"aggs":[{"oql":{"field":["target_fetched","equals", "true"]},"name":"agg_name"}]}',
            'logs': '{"aggs":[{"oql":{"and":[{"field":["event_is_bot_hit","equals","true"]}]},"name":"agg_name"}]}'
        },
        'export' : {
            'pages' : '{"oql":{"field":["fetched","equals", "true"]}}',
            'links' : '{"oql":{"field":["target_fetched","equals", "true"]}}',
            'logs' : '{"oql":{"field":["event_is_bot_hit","equals", "true"]}}'
        }
    }

    let oql = scope.config.oql;
    if(!oql || reset)
    {
        oql = oql_templates[scope.config.data_action][scope.config.index]
    }
    
    let oql_parsed = JSON.parse(oql);
    
    //add agg name if missing or rebuild aggs if missing oql node
    if(scope.config.data_action == 'aggs')
    {

        if(Object.keys(oql_parsed).indexOf('aggs') < 0 || oql_parsed.aggs.length == 0)
        {
            oql = oql_templates[scope.config.data_action][scope.config.index];
            oql_parsed = JSON.parse(oql);
        }
        else
        {
            //alert(JSON.stringify(oql_parsed.aggs))
            let agg_name = []
            for(let i=0; i<oql_parsed.aggs.length; i++)
            {
                if(Object.keys(oql_parsed.aggs[i]).indexOf('oql') < 0 || Object.keys(oql_parsed.aggs[i].oql).length == 0)
                {
                    throw 'OQL node missing or empty';
                }
                if(Object.keys(oql_parsed.aggs[i]).indexOf('name') < 0 || oql_parsed.aggs[i].name == '')
                {
                    oql_parsed.aggs[i].name = 'agg_'+i;
                }
               
                if(agg_name.indexOf(oql_parsed.aggs[i].name) < 0)
                {
                    agg_name.push(oql_parsed.aggs[i].name);
                }
                else
                {
                    oql_parsed.aggs[i].name = oql_parsed.aggs[i].name +' _ '+(Math.floor(Math.random() * 100) + 1 + i);
                }
            }
        }
    }
    else
    {
       if(Object.keys(oql_parsed).indexOf('oql') < 0 || Object.keys(oql_parsed.oql).length == 0)
        {
            oql = oql_templates[scope.config.data_action][scope.config.index]
            oql_parsed = JSON.parse(oql)
        }
    }

    //add date fields for logs
    if(scope.config.index == 'logs')
    {

        // analyze oql and add or update dates
        if(scope.config.data_action == 'aggs')
        {
            for(let i=0; i<oql_parsed.aggs.length; i++)
            {
                new_oql_node = add_logsdate_field(oql_parsed.aggs[i].oql, scope)
                oql_parsed.aggs[i].oql = new_oql_node;
                
                oql_parsed.aggs.splice(i, 1, oql_parsed.aggs[i]);
            }
            oql = oql_parsed;
        }
        else
        {
            oql = {"oql": add_logsdate_field(oql_parsed.oql, scope)}
        }
    }
    else
    {
        oql = oql_parsed;
    }
    
    return prettyPrint(oql)
}

function add_logsdate_field(oql, scope)
{

    oql_content = []
    //find and => add if null
    if(Object.keys(oql)[0] != 'and')
    {
        if(oql.field[0] != 'event_day' && oql.field[0] != 'event_datetime')
        {
            oql_content.push(oql)
        }
    }
    else
    {
        //update oql to update date_start_yyyy_mm_dd and date_end_yyyy_mm_dd
        for (let i=0; i<oql.and.length; i++)
        {
            if(
                (oql.and[i].field && oql.and[i].field[0] != 'event_day' && oql.and[i].field[0] != 'event_datetime')
                || (oql.and[i].and || oql.and[i].or))
            {
                oql_content.push(oql.and[i])
            }
        }
        
    }
    
    oql = {"and": [{"field": ["event_day", "between", [scope.date_start_yyyy_mm_dd, scope.date_end_yyyy_mm_dd]]},]}
    oql.and = oql.and.concat(oql_content);

    return oql
}

// return all except if there is only one crawl ==> return its ids
function selectDefaultCrawls(scope)
{
    let list = null
    if(scope.config.list_configs_crawls[scope.config.crawl_config])
    {
        list = 'all'
        if(scope.config.crawls_id)
            list = scope.config.crawls_id
        if(scope.config.list_configs_crawls[scope.config.crawl_config].length == 1 && list != 'last')
        {
            list = scope.config.list_configs_crawls[scope.config.crawl_config][0];
        }
    }
    
    return list
}


