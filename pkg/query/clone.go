package query

import "reflect"

// CloneStatement returns a deep copy of stmt.
func CloneStatement(stmt Statement) Statement {
	if stmt == nil {
		return nil
	}
	cloned := cloneASTValue(reflect.ValueOf(stmt))
	if !cloned.IsValid() || cloned.IsNil() {
		return nil
	}
	return cloned.Interface().(Statement)
}

// CloneExpression returns a deep copy of expr.
func CloneExpression(expr Expression) Expression {
	if expr == nil {
		return nil
	}
	cloned := cloneASTValue(reflect.ValueOf(expr))
	if !cloned.IsValid() || cloned.IsNil() {
		return nil
	}
	return cloned.Interface().(Expression)
}

func cloneASTValue(v reflect.Value) reflect.Value {
	if !v.IsValid() {
		return v
	}

	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		cloned := cloneASTValue(v.Elem())
		out := reflect.New(v.Type()).Elem()
		out.Set(cloned)
		return out
	case reflect.Ptr:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.New(v.Type().Elem())
		out.Elem().Set(cloneASTValue(v.Elem()))
		return out
	case reflect.Struct:
		out := reflect.New(v.Type()).Elem()
		for i := 0; i < v.NumField(); i++ {
			out.Field(i).Set(cloneASTValue(v.Field(i)))
		}
		return out
	case reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			out.Index(i).Set(cloneASTValue(v.Index(i)))
		}
		return out
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			out.SetMapIndex(cloneASTValue(iter.Key()), cloneASTValue(iter.Value()))
		}
		return out
	default:
		return v
	}
}
