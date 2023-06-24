UPDATE_MK_INCLUDE := false
UPDATE_MK_INCLUDE_AUTO_MERGE := false

include ./mk-include/cc-begin.mk
include ./mk-include/cc-vault.mk
include ./mk-include/cc-end.mk

### BEGIN MK-INCLUDE/ BOOTSTRAP ###
CURL = curl
FIND = find
JQ = jq
SED = sed
TAR = tar

CURL_LOCATION = $(CURL) --fail --silent --netrc --location
CURL_LATEST = $(CURL_LOCATION) $(GITHUB_API_CC_MK_INCLUDE_LATEST)
CURL_RELEASES = $(CURL_LOCATION) $(GITHUB_API_CC_MK_INCLUDE_RELEASES)

# Mount netrc so curl can work from inside a container
DOCKER_NETRC_MOUNT=1

GITHUB_API = https://api.github.com
GITHUB_API_CC_MK_INCLUDE = $(GITHUB_API)/repos/$(GITHUB_OWNER)/$(GITHUB_REPO)
GITHUB_API_CC_MK_INCLUDE_LATEST = $(GITHUB_API_CC_MK_INCLUDE_RELEASES)/latest
GITHUB_API_CC_MK_INCLUDE_RELEASES = $(GITHUB_API_CC_MK_INCLUDE)/releases
GITHUB_OWNER = confluentinc
GITHUB_REPO = cc-mk-include

JQ_RELEASE_TARBALL = $(JQ) --raw-output '.tarball_url'
JQ_LATEST_TARBALL = $(JQ) --raw-output 'sort_by(.published_at) | .[-1].tarball_url'

MK_INCLUDE_DIR = mk-include
MK_INCLUDE_LOCKFILE = .mk-include-lockfile
# You should adjust MK_INCLUDE_TIMEOUT_MINS to a little longer than the worst case cold build time for this repo.
MK_INCLUDE_TIMEOUT_MINS = 240
MK_INCLUDE_TIMESTAMP_FILE = .mk-include-timestamp

SED_RELEASE_TARBALL = $(SED) -n '/"tarball_url"/{s/^.*: *"//;s/",*//;p;q;}'

# Make sure we always have a copy of the latest cc-mk-include release from
# less than $(MK_INCLUDE_TIMEOUT_MINS) ago:
./$(MK_INCLUDE_DIR)/%.mk: .mk-include-check-FORCE
	@trap "rm -f $(MK_INCLUDE_LOCKFILE); exit" 0 2 3 15; \
	waitlock=0; while ! ( set -o noclobber; echo > $(MK_INCLUDE_LOCKFILE) ); do \
	   sleep $$waitlock; waitlock=`expr $$waitlock + 1`; \
	   test 14 -lt $$waitlock && { \
	      echo 'stealing stale lock after 105s' >&2; \
	      break; \
	   } \
	done; \
	test -s $(MK_INCLUDE_TIMESTAMP_FILE) || rm -f $(MK_INCLUDE_TIMESTAMP_FILE); \
	test -z "`$(FIND) $(MK_INCLUDE_TIMESTAMP_FILE) -mmin +$(MK_INCLUDE_TIMEOUT_MINS) 2>&1`" || { \
	   grep -q 'machine api.github.com' ~/.netrc 2>/dev/null || { \
	      echo 'error: follow https://confluentinc.atlassian.net/l/cp/0WXXRLDh to fix your ~/.netrc'; \
	      exit 1; \
	   }; \
	   retries=0; while test 5 -gt $$retries; do \
	      sleep `expr $$retries '*' $$retries '*' $$retries`; \
	      tarball=`$(CURL_LATEST) | $(SED_RELEASE_TARBALL)`; \
	      $(CURL_LOCATION) "$$tarball" --output $(MK_INCLUDE_TIMESTAMP_FILE) && break; \
	      retries=`expr 1 + $$retries`; \
	   done; test 5 -gt $$retries \
	   && $(TAR) zxf $(MK_INCLUDE_TIMESTAMP_FILE) \
	   && rm -rf $(MK_INCLUDE_DIR) \
	   && mv $(GITHUB_OWNER)-$(GITHUB_REPO)-* $(MK_INCLUDE_DIR) \
	   && echo installed $$tarball from $(GITHUB_REPO) \
	   ; \
	} || { \
	   echo 'unable to access $(GITHUB_REPO) fetch API to check for latest release; next try in $(MK_INCLUDE_TIMEOUT_MINS) minutes'; \
	   test -f $(MK_INCLUDE_TIMESTAMP_FILE) && touch $(MK_INCLUDE_TIMESTAMP_FILE); \
	}

.mk-include-check-FORCE:
### END MK-INCLUDE/ BOOTSTRAP ###
