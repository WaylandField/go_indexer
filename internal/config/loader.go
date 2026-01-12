package config

func LoadFromEnv() (Config, error) {
	if err := loadDotEnv(); err != nil {
		return Config{}, err
	}
	return Load(FromEnviron())
}
