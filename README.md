Ots for Yii 2
==============================================

This extension provides the [ots](https://help.aliyun.com/document_detail/27280.html?spm=5176.7727283.6.539.MdnSPT) integration for the [Yii framework 2.0](http://www.yiiframework.com).
It includes basic querying/search support and also implements the `ActiveRecord` pattern that allows you to store active
records in elasticsearch.

For license information check the [LICENSE](LICENSE.md)-file.

Documentation is at [docs/guide/README.md](docs/guide/README.md).

Requirements
------------

This extension works with elasticsearch version 1.0 to 4.x. elasticsearch 5.0 is currently not supported.

Installation
------------

The preferred way to install this extension is through [composer](http://getcomposer.org/download/).

Either run

```
php composer.phar require --prefer-dist sabercoding/yii2-ots
```

to the require section of your composer.json.

Configuration
-------------

To use this extension, you have to configure the Connection class in your application configuration:

```php
return [
    //....
    'components' => [
        'ots' => [

        ],
    ]
];
```
