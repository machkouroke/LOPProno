wget "https://github.com/datopian/data-cli/releases/download/v0.9.5/data-linux.gz"
gunzip data-linux.gz
chmod +x data-linux
sudo mv data-linux /usr/local/bin/data
for data in spanish-la-liga italian-serie-a french-ligue-1 german-bundesliga; do
  data get "https://datahub.io/sports-data/$data"
done
