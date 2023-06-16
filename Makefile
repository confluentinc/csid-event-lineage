UPDATE_MK_INCLUDE := false
UPDATE_MK_INCLUDE_AUTO_MERGE := false

include ./mk-include/cc-begin.mk
include ./mk-include/cc-vault.mk
include ./mk-include/cc-end.mk

### BEGIN MK-INCLUDE/ BOOTSTRAP ###
CURL = curl
FIND = find
SED = sed
TAR = tar

MK_INCLUDE_DIR = mk-include
# You should adjust MK_INCLUDE_TIMEOUT_MINS to a little longer than the worst case cold build time for this repo.
MK_INCLUDE_TIMEOUT_MINS = 240
MK_INCLUDE_TIMESTAMP_FILE = .mk-include-timestamp

GITHUB_API = https://api.github.com
GITHUB_API_CC_MK_INCLUDE = $(GITHUB_API)/repos/$(GITHUB_OWNER)/$(GITHUB_REPO)
GITHUB_API_CC_MK_INCLUDE_LATEST = $(GITHUB_API_CC_MK_INCLUDE)/releases/latest
GITHUB_OWNER = confluentinc
GITHUB_REPO = cc-mk-include

# Make sure we always have a copy of the latest cc-mk-include release from
# less than $(MK_INCLUDE_TIMEOUT_MINS) ago:
./$(MK_INCLUDE_DIR)/%.mk: .mk-include-check-FORCE
	@test -z "`$(FIND) $(MK_INCLUDE_TIMESTAMP_FILE) -mmin +$(MK_INCLUDE_TIMEOUT_MINS) 2>&1`" || { \
	   $(CURL) --silent --netrc --location $(GITHUB_API_CC_MK_INCLUDE_LATEST) \
	      |$(SED) -n '/"tarball_url"/{s/^.*: *"//;s/",*//;p;q;}' \
	      |xargs $(CURL) --silent --netrc --location --output $(MK_INCLUDE_TIMESTAMP_FILE) \
	   && $(TAR) zxf $(MK_INCLUDE_TIMESTAMP_FILE) \
	   && rm -rf $(MK_INCLUDE_DIR) \
	   && mv $(GITHUB_OWNER)-$(GITHUB_REPO)-* $(MK_INCLUDE_DIR) \
	   && echo installed latest $(GITHUB_REPO) release \
	   ; \
	} || { \
	   echo 'unable to access $(GITHUB_REPO) fetch API to check for latest release; next try in $(MK_INCLUDE_TIMEOUT_MINS) minutes'; \
	   touch $(MK_INCLUDE_TIMESTAMP_FILE); \
	}

.mk-include-check-FORCE:
### END MK-INCLUDE/ BOOTSTRAP ###
