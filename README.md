[`pure | 📦`](https://github.com/telamon/create-pure)
[`code style | standard`](https://standardjs.com/)
# 🕷️🤖 hyperspace-indexer

> Index hyper:// links

Builds an hyperdrive powered redistributable phonebook that can be queried offline.

Database layout
```
about/{drive.key} # => { title: '...', description: '...', version: 3, seeds: 500 }
backlinks/{linkTarget.key}/{drive.key} # => /path/file:lineNumber
updates/{date}/{utc}_{drive.key}_{path+file} # => {version: '', title: '', ellipsis: ''}
ngrams/h/e/l/l/o/{drive.key} #=> /path/file:lineNumber
filelists/{drive.key} # => line-separated list of files. (mounts excluded)
```

## Use

Step 1. Clone this repository

Step 2. Run:

```bash
$ node index.js
```

A drive `dist/` should be generated containing all analyzed indexes,
Also the file `database.url` should be generated containing the `hyper://...` URL to the database.

## Donations

```ad
 _____                      _   _           _
|  __ \   Help Wanted!     | | | |         | |
| |  | | ___  ___ ___ _ __ | |_| |     __ _| |__  ___   ___  ___
| |  | |/ _ \/ __/ _ \ '_ \| __| |    / _` | '_ \/ __| / __|/ _ \
| |__| |  __/ (_|  __/ | | | |_| |___| (_| | |_) \__ \_\__ \  __/
|_____/ \___|\___\___|_| |_|\__|______\__,_|_.__/|___(_)___/\___|

If you're reading this it means that the docs are missing or in a bad state.

Writing and maintaining friendly and useful documentation takes
effort and time. In order to do faster releases
I will from now on provide documentation relational to project activity.

  __How_to_Help____________________________________.
 |                                                 |
 |  - Open an issue if you have ANY questions! :)  |
 |  - Star this repo if you found it interesting   |
 |  - Fork off & help document <3                  |
 |.________________________________________________|

I publish all of my work as Libre software and will continue to do so,
drop me a penny at Patreon to help fund experiments like these.

Patreon: https://www.patreon.com/decentlabs
Discord: https://discord.gg/K5XjmZx
Telegram: https://t.me/decentlabs_se
```


## Changelog

### 0.1.0 first release

## Contributing

By making a pull request, you agree to release your modifications under
the license stated in the next section.

Only changesets by human contributors will be accepted.

## License

[AGPL-3.0-or-later](./LICENSE)

2020 &#x1f12f; Decent Labs AB - Tony Ivanov
