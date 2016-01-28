This plugin provides two recipes, to connect to two APIs about movies:

- <a href="http://www.omdbapi.com">OMDb</a> requires the title to be rather close to the original title (for instance it won't find “Oceans 13”). It fetches IMDb, metacritic and rotten tomatoes scores.
- <a href="https://www.themoviedb.org/">TMDb</a> requires an API key (freely available by registering on their website). It finds approximate titles in foreign languages. It has many features (see their website), but for now we implemented only title search and query by imdb id.

To enable the TMDb recipe, after installing the plugin,
please go to Administration → plugins → installed → movies-api → settings and enter your TMDb API key.
