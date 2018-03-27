package ca

type ServerConfig struct {
	rootPath       string
	caDir          string
}

type PKIConfig struct {
	caOrganization string
	caCountry      string
}
