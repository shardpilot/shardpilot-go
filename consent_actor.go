package shardpilot

// Consent actor kinds: the identity domain a consent receipt covers, sent as
// the receipt's "kind".
const (
	consentKindAnon         = "anon"
	consentKindUserVerified = "user_verified"
)

// consentActor is the actor one consent decision covers: the identifier its
// receipt carries and the kind of identity that identifier is.
type consentActor struct {
	identifier string
	kind       string
}

// selectConsentActor is the one consent actor rule; every decision, receipt,
// record scope and floor actor check uses it.
//
// A client source authenticates with a publishable key, so its UserID is
// self-asserted and never a consent key: the actor is the anonymous ID, kind
// anon. A server or backend source vouches for its user: the actor is the
// UserID, kind user_verified, else the anonymous ID, kind anon.
//
// refused reports a client source with a UserID and no AnonymousID. It has no
// admissible consent actor: grants are refused and denials stay local. No
// identifier at all selects nothing and is not refused (the local-only path).
func selectConsentActor(source Source, userID, anonymousID string) (actor consentActor, refused bool) {
	if source == SourceClient {
		if anonymousID == "" {
			return consentActor{}, userID != ""
		}
		return consentActor{identifier: anonymousID, kind: consentKindAnon}, false
	}
	if userID != "" {
		return consentActor{identifier: userID, kind: consentKindUserVerified}, false
	}
	if anonymousID != "" {
		return consentActor{identifier: anonymousID, kind: consentKindAnon}, false
	}
	return consentActor{}, false
}

// consentActor selects the configured consent actor.
func (c *Client) consentActor() (consentActor, bool) {
	return selectConsentActor(c.cfg.Source, c.cfg.UserID, c.cfg.AnonymousID)
}

// consentFloorUserScoped reports whether the floor's decision does not cover
// the anonymous identity: the configured actor is a verified user, or a client
// source has no admissible actor. The SDK's own experiment facts ride the
// anonymous ID alone, so such a decision does not cover them.
func (c *Client) consentFloorUserScoped() bool {
	actor, refused := c.consentActor()
	return refused || actor.kind == consentKindUserVerified
}

// validConsentKind reports whether kind is one this SDK mints.
func validConsentKind(kind string) bool {
	return kind == consentKindAnon || kind == consentKindUserVerified
}
