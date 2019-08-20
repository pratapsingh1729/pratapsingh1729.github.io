#docker container exec jekyllblog jekyll serve --watch --drafts
#docker container run --name jekyllblog -v "$PWD:/srv/jekyll" -p 4000:4000 -it jekyll/jekyll jekyll serve --watch --drafts
docker container run --rm -v "$PWD:/srv/jekyll" -p 4000:4000 -it jekyll/jekyll jekyll serve --watch --drafts