FROM php:7.1-cli

RUN pecl install xdebug-2.6.0 \
    && docker-php-ext-enable xdebug

RUN php -r "copy('https://getcomposer.org/installer', 'composer-setup.php');" \
    && php composer-setup.php --install-dir=/usr/bin --filename=composer \
    && php -r "unlink('composer-setup.php');"

WORKDIR "/app"
