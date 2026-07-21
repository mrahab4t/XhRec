#Clone your new Gitea repo:
#git clone <url-of-your-gitea-migrated-repo>
#cd <repo-name>

#Add GitHub as upstream:
#git remote add upstream <url-of-original-github-repo>

#Verify remotes:
#git remote -v
# Output should show 'origin' (Gitea) and 'upstream' (GitHub)

#Sync changes (whenever needed):
git fetch upstream
git merge upstream/main  # Or rebase
git push origin main
