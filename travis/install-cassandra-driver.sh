apt-get install g++ make cmake libuv-dev libssl-dev libgmp-dev php5 php5-dev openssl libpcre3-dev git -y
git clone https://github.com/datastax/php-driver.git
cd php-driver
git submodule update --init
cd ext
sudo ./install.sh
echo -e "; DataStax PHP Driver\nextension=cassandra.so" >> `php --ini | grep "Loaded Configuration" | sed -e "s|.*:\s*||"`
